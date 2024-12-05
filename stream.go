package pipeline

import (
	"context"
	"sync"
)

// channel(<group1, id1>) -> Streamer() -> channel(<group2, id2>)
// 利用者が実装する型
type Streamer[I Record, O Record] interface {
	Stream(ctx context.Context, inputs <-chan I) (<-chan O, <-chan error)
}

// 内部処理で利用される、具体型を持ったinterface
type streamer interface {
	Stream(ctx context.Context, inputs <-chan Record) (<-chan Record, <-chan error)
}

// Reducerをラップしてreducerインターフェースを実装する型
type streamWrapper[I Record, O Record] struct {
	streamer Streamer[I, O]
}

func (w *streamWrapper[I, O]) Stream(ctx context.Context, inputs <-chan Record) (<-chan Record, <-chan error) {
	ins := make(chan I)
	go func() {
		for i := range inputs {
			ins <- i.(I)
		}
		close(ins)
	}()

	outs, errs := w.streamer.Stream(ctx, ins)

	outputs := make(chan Record)
	go func() {
		defer close(outputs)
		for o := range outs {
			outputs <- o
		}
	}()

	return outputs, errs
}

type streamProcessor struct {
	name     string
	streamer streamer
}

func newStreamProcessor[I Record, O Record](name string, streamer Streamer[I, O]) *streamProcessor {
	return &streamProcessor{
		name:     name,
		streamer: &streamWrapper[I, O]{streamer: streamer},
	}
}

func (p *streamProcessor) SetMaxParallel(max int) {
	panic("max parallel is not supported for stream")
}

func (p *streamProcessor) SetAbortIfAnyError(value bool) {
	panic("abortIfAnyError is not supported for stream")
}

func (p *streamProcessor) Name() string {
	return p.name
}

func (p *streamProcessor) Type() ProcessorType {
	return ProcessorTypeStream
}

func (p *streamProcessor) Process(ctx context.Context, inputs <-chan Record, abort chan<- error) <-chan Output {
	outputs := make(chan Output)

	go func() {
		defer close(outputs)

		outs, errs := p.streamer.Stream(ctx, inputs)

		wg := sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for o := range outs {
				outputs <- Output{
					Status:  OutputStatusSuccess,
					Records: []Record{o},
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for err := range errs {
				if IsAbortError(err) {
					abort <- err
					continue
				}

				outputs <- Output{
					Status: OutputStatusError,
					Err:    err,
				}
			}
		}()

		done := make(chan struct{})
		go func() {
			defer close(done)

			wg.Wait()
			done <- struct{}{}
		}()

		select {
		case <-ctx.Done():
			abort <- ctx.Err()
		case <-done:
		}
	}()

	return outputs
}
