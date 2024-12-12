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
func MapStage[I Record, O Record](name string, mapper Mapper[I, O], opts ...PipelineStageOption) *PipelineStage {
	return Stage(newMapProcessor(name, mapper), opts...)
}

func ReduceStage[I Record, O Record, G Group](name string, reducer Reducer[I, O, G], opts ...PipelineStageOption) *PipelineStage {
	return Stage(newReduceProcessor(name, reducer), opts...)
}

func StreamStage[I Record, O Record](name string, streamer Streamer[I, O]) *PipelineStage {
	return Stage(newStreamProcessor(name, streamer))
}

/* 実行時オプション */
func StageMaxParallel(max int) PipelineStageOption {
	return func(s *PipelineStage) {
		s.processor.SetMaxParallel(max)
	}
}

// Deprecated: 代わりにAbortErrorを利用してください
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
	Name       string
	Type       ProcessorType
	GroupCount int
	Outputs    []SummarizedOutput
}
