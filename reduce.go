package pipeline

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// <group1, list(id1)> -> Reduce() -> list(<group2, id2>)
// 利用者が実装する型
type Reducer[I Record, O Record, G Group] interface {
	Reduce(ctx context.Context, group G, inputs []I) ([]O, error)
}

// 内部処理で利用される、具体型を持ったinterface
type reducer interface {
	Reduce(ctx context.Context, group Group, inputs []Record) ([]Record, error)
}

// Reducerをラップしてreducerインターフェースを実装する型
type reduceWrapper[I Record, O Record, G Group] struct {
	reducer Reducer[I, O, G]
}

func (w *reduceWrapper[I, O, G]) Reduce(ctx context.Context, group Group, inputs []Record) ([]Record, error) {
	g := group.(G)

	var ins []I
	for _, i := range inputs {
		ins = append(ins, i.(I))
	}

	outs, err := w.reducer.Reduce(ctx, g, ins)
	if err != nil {
		return nil, err
	}

	var outputs []Record
	for _, o := range outs {
		outputs = append(outputs, o)
	}

	return outputs, nil
}

type reduceProcessor struct {
	name            string
	reducer         reducer
	maxParallel     int
	abortIfAnyError bool
}

type ReducerOption func(p *reduceProcessor)

func (o ReducerOption) ReduceStageOption() {}

func newReduceProcessor[I Record, O Record, G Group](name string, reducer Reducer[I, O, G], opts ...ReducerOption) *reduceProcessor {
	p := &reduceProcessor{
		name: name,
		reducer: &reduceWrapper[I, O, G]{
			reducer: reducer,
		},
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *reduceProcessor) SetMaxParallel(max int) {
	p.maxParallel = max
}

func (p *reduceProcessor) SetAbortIfAnyError(value bool) {
	p.abortIfAnyError = value
}

func (p *reduceProcessor) Type() ProcessorType {
	return ProcessorTypeReduce
}

func (p *reduceProcessor) Name() string {
	return p.name
}

func (p *reduceProcessor) Process(ctx context.Context, inputs <-chan Record, abort chan<- error) <-chan Output {
	outputs := make(chan Output)

	eg, ctx := errgroup.WithContext(ctx)
	if p.maxParallel > 0 {
		eg.SetLimit(p.maxParallel)
	}

	type group struct {
		group Group
		done  bool
	}

	go func() {
		groups := map[string]*group{}
		groupedInputs := map[string][]Record{}
		for in := range inputs {
			gr := in.Group().String()

			if g, ok := groups[gr]; ok {
				// すでにコミットされたグループは無視する
				if g.done {
					continue
				}
			} else {
				// 新しいグループの場合はグループ一覧に追加する
				groups[gr] = &group{
					group: in.Group(),
					done:  false,
				}
			}

			// GroupCommitが流れてきた場合、すぐにgroupの処理を開始して、レコードをmapから削除する
			// こうすることで、必要以上にメモリを使用しないようにする
			if _, ok := in.(groupCommit); ok {
				groups[gr].done = true
				inputs := groupedInputs[gr]
				delete(groupedInputs, gr)

				eg.Go(func() error {
					output, err := p.reduce(ctx, in.Group(), inputs)
					if err != nil {
						return err
					}
					outputs <- output
					return nil
				})
			} else {
				groupedInputs[gr] = append(groupedInputs[gr], in)
			}
		}

		// 全てのレコードを読んだら、GroupCommitされていないレコードを順に処理する
		for _, group := range groups {
			if group.done {
				continue
			}

			gr := group.group.String()

			groups[gr].done = true
			inputs := groupedInputs[gr]
			delete(groupedInputs, gr)

			eg.Go(func() error {
				output, err := p.reduce(ctx, group.group, inputs)
				if err != nil {
					return err
				}
				outputs <- output
				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			abort <- err
		}
		close(outputs)
	}()

	return outputs
}

func (p *reduceProcessor) reduce(ctx context.Context, group Group, inputs []Record) (output Output, err error) {
	defer func() {
		// abortIfAnyErrorがfalseの場合は、errを返す代わりにエラーステータスを持った通常レコードを返す
		if err != nil && !p.abortIfAnyError && !IsAbortError(err) {
			output = Output{
				Unit:   group.String(),
				Status: OutputStatusError,
				Err:    err,
			}
			err = nil
		}
	}()

	select {
	case <-ctx.Done():
		return Output{}, ctx.Err()
	default:
	}

	o, err := p.reducer.Reduce(ctx, group, inputs)
	if err != nil {
		return Output{}, err
	}

	return Output{
		Unit:    group.String(),
		Status:  OutputStatusSuccess,
		Records: o,
	}, nil
}
