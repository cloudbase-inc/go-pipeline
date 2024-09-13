package pipeline

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// <group1, id1> -> Mapper() -> list(<group2, id2>)
type Mapper interface {
	Map(ctx context.Context, input Record) ([]Record, error)
}

type mapProcessor struct {
	name            string
	mapper          Mapper
	maxParallel     int
	abortIfAnyError bool
}

func newMapProcessor(name string, mapper Mapper) *mapProcessor {
	return &mapProcessor{
		name:   name,
		mapper: mapper,
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
			if _, ok := in.(groupCommit); ok {
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
