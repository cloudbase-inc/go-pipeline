package pipeline

import (
	"context"
	"sync"
)

type Pipeline struct {
	stages []*PipelineStage
}

func New(stages ...*PipelineStage) *Pipeline {
	return &Pipeline{
		stages: stages,
	}
}

func (p *Pipeline) Execute(ctx context.Context) (outputs []Record, stages []StageExecution, abortErr error) {
	originInputs := make(chan Record)
	go func() {
		originInputs <- originInput{}
		close(originInputs)
	}()

	stageInputs := []chan Record{originInputs}
	for range p.stages {
		stageInputs = append(stageInputs, make(chan Record))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	abortWg := sync.WaitGroup{}
	abort := make(chan error)

	abortWg.Add(1)
	go func() {
		defer abortWg.Done()

		if err, ok := <-abort; ok {
			abortErr = err
			cancel()
		}
		for range abort {
			// 書き込みでブロックされないよう、2件目以降のabortは無視する
		}
	}()

	for i, stage := range p.stages {
		go func() {
			if stage.timeout > 0 {
				ctxTimeout, cancel := context.WithTimeout(ctx, stage.timeout)
				defer cancel()

				ctx = ctxTimeout
			}

			pr := stage.processor

			summarizedOutputs := []SummarizedOutput{}
			for o := range pr.Process(ctx, stageInputs[i], abort) {
				// 前段のoutputを、次のinputに入れる
				for _, r := range o.Records {
					stageInputs[i+1] <- r
				}
				summarizedOutputs = append(summarizedOutputs, o.Summarized())
			}

			// 前段のinputsがcloseしない限り次のstageが終わることはないので、
			// ここは排他制御する必要はない
			stages = append(stages, StageExecution{
				Name:    pr.Name(),
				Type:    pr.Type(),
				Outputs: summarizedOutputs,
			})

			close(stageInputs[i+1])
		}()
	}

	outputs = []Record{}
	for in := range stageInputs[len(p.stages)] {
		outputs = append(outputs, in)
	}

	// abortエラーの取りこぼしを防ぐため、abortゴルーチンの完了を待つ
	close(abort)
	abortWg.Wait()

	if abortErr != nil {
		return nil, nil, abortErr
	}

	return outputs, stages, nil
}
