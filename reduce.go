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

	go func() {
		groups := map[string]Group{}
		groupedInputs := map[string][]Record{}
		for in := range inputs {
			gr := in.Group().String()

			groups[gr] = in.Group()
			if _, ok := in.(emptyGroup); !ok {
				groupedInputs[gr] = append(groupedInputs[gr], in)
			}
		}

		for _, group := range groups {
			eg.Go(func() (err error) {
				defer func() {
					if err != nil {
						outputs <- Output{
							Unit:   group.String(),
							Status: OutputStatusError,
							Err:    err,
						}
						// abortIfAnyErrorがtrueの場合のみ、errを返して全体を止める
						if !p.abortIfAnyError {
							err = nil
						}
					}
				}()

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				o, err := p.reducer.Reduce(ctx, group, groupedInputs[group.String()])
				if err != nil {
					return err
				}

				outputs <- Output{
					Unit:    group.String(),
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
