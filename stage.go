package pipeline

import "time"

type PipelineStage struct {
	processor Processor
	timeout   time.Duration
}

type PipelineStageOption func(*PipelineStage)

/* Stageの初期化 */
func Stage(pr Processor, opts ...PipelineStageOption) *PipelineStage {
	s := &PipelineStage{
		processor: pr,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Mapper / Reducerを元にステージを組み立てるためのユーティリティ関数
func MapStage(name string, mapper Mapper, opts ...PipelineStageOption) *PipelineStage {
	return Stage(newMapProcessor(name, mapper), opts...)
}

func ReduceStage(name string, reducer Reducer, opts ...PipelineStageOption) *PipelineStage {
	return Stage(newReduceProcessor(name, reducer), opts...)
}

/* 実行時オプション */
func StageMaxParallel(max int) PipelineStageOption {
	return func(s *PipelineStage) {
		s.processor.SetMaxParallel(max)
	}
}

func StageAbortIfAnyError(value bool) PipelineStageOption {
	return func(s *PipelineStage) {
		s.processor.SetAbortIfAnyError(value)
	}
}

func StageTimeout(timeout time.Duration) PipelineStageOption {
	return func(s *PipelineStage) {
		s.timeout = timeout
	}
}

// ステージの実行結果
type StageExecution struct {
	Name    string
	Type    ProcessorType
	Outputs []SummarizedOutput
}
