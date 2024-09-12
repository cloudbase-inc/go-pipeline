package pipeline

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// <group1, list(id1)> -> Reduce() -> list(<group2, id2>)
type Reducer interface {
	Reduce(ctx context.Context, group Group, inputs []Record) ([]Record, error)
}

type reduceProcessor struct {
	name            string
	reducer         Reducer
	maxParallel     int
	abortIfAnyError bool
}

type ReducerOption func(p *reduceProcessor)

func (o ReducerOption) ReduceStageOption() {}

func newReduceProcessor(name string, reducer Reducer, opts ...ReducerOption) *reduceProcessor {
	p := &reduceProcessor{
		name:    name,
		reducer: reducer,
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

			if _, ok := groups[gr]; !ok {
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
		if err != nil && !p.abortIfAnyError {
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
