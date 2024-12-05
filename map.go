package pipeline

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// <group1, id1> -> Mapper() -> list(<group2, id2>)
// 利用者が実装する型
type Mapper[I Record, O Record] interface {
	Map(ctx context.Context, input I) ([]O, error)
}

// 内部処理で利用される、具体型を持ったinterface
type mapper interface {
	Map(ctx context.Context, input Record) ([]Record, error)
}

// Mapperをラップしてmapperインターフェースを実装する型
type mapWrapper[I Record, O Record] struct {
	mapper Mapper[I, O]
}

func (w *mapWrapper[I, O]) Map(ctx context.Context, input Record) ([]Record, error) {
	i := input.(I)

	outs, err := w.mapper.Map(ctx, i)
	if err != nil {
		return nil, err
	}

	var outputs []Record
	for _, o := range outs {
		outputs = append(outputs, o)
	}

	return outputs, nil
}

type mapProcessor struct {
	name            string
	mapper          mapper
	maxParallel     int
	abortIfAnyError bool
}

func newMapProcessor[I Record, O Record](name string, mapper Mapper[I, O]) *mapProcessor {
	return &mapProcessor{
		name: name,
		mapper: &mapWrapper[I, O]{
			mapper: mapper,
		},
	}
}

func (p *mapProcessor) SetMaxParallel(max int) {
	p.maxParallel = max
}

func (p *mapProcessor) SetAbortIfAnyError(value bool) {
	p.abortIfAnyError = value
}

func (p *mapProcessor) Name() string {
	return p.name
}

func (p *mapProcessor) Type() ProcessorType {
	return ProcessorTypeMap
}

func (p *mapProcessor) Process(ctx context.Context, inputs <-chan Record, abort chan<- error) <-chan Output {
	outputs := make(chan Output)

	eg, ctx := errgroup.WithContext(ctx)
	if p.maxParallel > 0 {
		eg.SetLimit(p.maxParallel)
	}

	go func() {
		for in := range inputs {
			// GroupCommitは無視する
			if isGroupCommit(in) {
				continue
			}

			eg.Go(func() (err error) {
				defer func() {
					if err != nil {
						outputs <- Output{
							Unit:   RecordKey(in),
							Status: OutputStatusError,
							Err:    err,
						}
						// abortIfAnyErrorがtrueの場合のみ、errを返して全体を止める
						if !p.abortIfAnyError && !IsAbortError(err) {
							err = nil
						}
					}
				}()

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				o, err := p.mapper.Map(ctx, in)
				if err != nil {
					return err
				}

				outputs <- Output{
					Unit:    RecordKey(in),
					Status:  OutputStatusSuccess,
					Records: o,
				}

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
