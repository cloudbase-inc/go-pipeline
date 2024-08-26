package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPipeline_Execute(t *testing.T) {
	type fields struct {
		stages []*PipelineStage
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantOutputs []Record
		wantStages  []StageExecution
		wantErr     error
	}{
		{
			name: "happy path",
			fields: fields{
				stages: []*PipelineStage{
					MapStage("Generator", &testGenerator{}),
					MapStage("Map1", &testMapper{}),
					MapStage("Map2", &testMapper{}),
					ReduceStage("Reduce", &testReducer{}),
				},
			},
			args: args{
				ctx: context.Background(),
			},
			wantOutputs: []Record{
				testRecord{"group1_mapped_mapped", "4"},
				testRecord{"group1_mapped_empty", "0"},
			},
			wantStages: []StageExecution{
				{
					Name: "Generator",
					Type: ProcessorTypeMap,
					Outputs: []SummarizedOutput{
						{
							Unit:        "*/*",
							Status:      OutputStatusSuccess,
							RecordCount: 2,
							GroupCount:  2,
						},
					},
				},
				{
					Name: "Map1",
					Type: ProcessorTypeMap,
					Outputs: []SummarizedOutput{
						{
							Unit:        "group1/id1",
							Status:      OutputStatusSuccess,
							RecordCount: 2,
							GroupCount:  2,
						},
						{
							Unit:   "error/id2",
							Status: OutputStatusError,
							Err:    errTestMapper,
						},
					},
				},
				{
					Name: "Map2",
					Type: ProcessorTypeMap,
					Outputs: []SummarizedOutput{
						{
							Unit:        "group1_mapped/id1_1",
							Status:      OutputStatusSuccess,
							RecordCount: 2,
							GroupCount:  2,
						},
						{
							Unit:        "group1_mapped/id1_2",
							Status:      OutputStatusSuccess,
							RecordCount: 2,
							GroupCount:  2,
						},
					},
				},
				{
					Name: "Reduce",
					Type: ProcessorTypeReduce,
					Outputs: []SummarizedOutput{
						{
							Unit:        "group1_mapped_mapped",
							Status:      OutputStatusSuccess,
							RecordCount: 1,
							GroupCount:  1,
						},
						{
							Unit:        "group1_mapped_empty",
							Status:      OutputStatusSuccess,
							RecordCount: 1,
							GroupCount:  1,
						},
					},
				},
			},
		},
		{
			name: "timeout (whole pipeline)",
			fields: fields{
				stages: []*PipelineStage{
					MapStage("Generator", &testGeneratorTimeout{}),
					MapStage("Map1", &testMapper{}, StageMaxParallel(1)),
					MapStage("Map2", &testMapper{}),
					ReduceStage("Reduce", &testReducer{}),
				},
			},
			args: args{
				ctx: func() context.Context {
					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					t.Cleanup(cancel)
					return ctx
				}(),
			},
			wantOutputs: []Record{},
			wantStages: []StageExecution{
				{
					Name: "Generator",
					Type: ProcessorTypeMap,
					Outputs: []SummarizedOutput{
						{
							Unit:        "*/*",
							Status:      OutputStatusSuccess,
							RecordCount: 4,
							GroupCount:  4,
						},
					},
				},
				{
					Name: "Map1",
					Type: ProcessorTypeMap,
					Outputs: []SummarizedOutput{
						{
							Unit:        "group1/id1",
							Status:      OutputStatusSuccess,
							RecordCount: 2,
							GroupCount:  2,
						},
						{
							Unit:   "error/id2",
							Status: OutputStatusError,
							Err:    errTestMapper,
						},
						{
							Unit:   "timeout/id3",
							Status: OutputStatusError,
							Err:    context.DeadlineExceeded,
						},
						{
							Unit:   "group4/id4",
							Status: OutputStatusError,
							Err:    context.DeadlineExceeded,
						},
					},
				},
				// mapperは前段で出力されたものを順次処理していくため、timeoutが発生したかどうかに関係なく処理を続行することができる
				{
					Name: "Map2",
					Type: ProcessorTypeMap,
					Outputs: []SummarizedOutput{
						{
							Unit:        "group1_mapped/id1_1",
							Status:      OutputStatusSuccess,
							RecordCount: 2,
							GroupCount:  2,
						},
						{
							Unit:        "group1_mapped/id1_2",
							Status:      OutputStatusSuccess,
							RecordCount: 2,
							GroupCount:  2,
						},
					},
				},
				// reducerは前段の全ての出力を待ってから処理を開始するため、timeoutが発生した場合はそのままreducerもタイムアウトになる
				{
					Name: "Reduce",
					Type: ProcessorTypeReduce,
					Outputs: []SummarizedOutput{
						{
							Unit:   "group1_mapped_mapped",
							Status: OutputStatusError,
							Err:    context.DeadlineExceeded,
						},
						{
							Unit:   "group1_mapped_empty",
							Status: OutputStatusError,
							Err:    context.DeadlineExceeded,
						},
					},
				},
			},
		},
		{
			name: "timeout (specific stage)",
			fields: fields{
				stages: []*PipelineStage{
					MapStage("Generator", &testGeneratorTimeout{}),
					MapStage("Map1", &testMapper{}, StageMaxParallel(1), StageTimeout(100*time.Millisecond)),
					MapStage("Map2", &testMapper{}),
					ReduceStage("Reduce", &testReducer{}),
				},
			},
			args: args{
				ctx: context.Background(),
			},
			wantOutputs: []Record{
				testRecord{"group1_mapped_mapped", "4"},
				testRecord{"group1_mapped_empty", "0"},
			},
			wantStages: []StageExecution{
				{
					Name: "Generator",
					Type: ProcessorTypeMap,
					Outputs: []SummarizedOutput{
						{
							Unit:        "*/*",
							Status:      OutputStatusSuccess,
							RecordCount: 4,
							GroupCount:  4,
						},
					},
				},
				{
					Name: "Map1",
					Type: ProcessorTypeMap,
					Outputs: []SummarizedOutput{
						{
							Unit:        "group1/id1",
							Status:      OutputStatusSuccess,
							RecordCount: 2,
							GroupCount:  2,
						},
						{
							Unit:   "error/id2",
							Status: OutputStatusError,
							Err:    errTestMapper,
						},
						{
							Unit:   "timeout/id3",
							Status: OutputStatusError,
							Err:    context.DeadlineExceeded,
						},
						{
							Unit:   "group4/id4",
							Status: OutputStatusError,
							Err:    context.DeadlineExceeded,
						},
					},
				},
				// Map1はステージ単位でタイムアウトになるが、後続の処理は正常に完了する
				{
					Name: "Map2",
					Type: ProcessorTypeMap,
					Outputs: []SummarizedOutput{
						{
							Unit:        "group1_mapped/id1_1",
							Status:      OutputStatusSuccess,
							RecordCount: 2,
							GroupCount:  2,
						},
						{
							Unit:        "group1_mapped/id1_2",
							Status:      OutputStatusSuccess,
							RecordCount: 2,
							GroupCount:  2,
						},
					},
				},
				{
					Name: "Reduce",
					Type: ProcessorTypeReduce,
					Outputs: []SummarizedOutput{
						{
							Unit:        "group1_mapped_mapped",
							Status:      OutputStatusSuccess,
							RecordCount: 1,
							GroupCount:  1,
						},
						{
							Unit:        "group1_mapped_empty",
							Status:      OutputStatusSuccess,
							RecordCount: 1,
							GroupCount:  1,
						},
					},
				},
			},
		},
		{
			name: "abort",
			fields: fields{
				stages: []*PipelineStage{
					MapStage("Generator", &testBrokenGenerator{}, StageAbortIfAnyError(true)),
				},
			},
			args: args{
				ctx: context.Background(),
			},
			wantErr: errTestBrokenGenerator,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pipeline{
				stages: tt.fields.stages,
			}
			outputs, stages, err := p.Execute(tt.args.ctx)
			if tt.wantErr != nil {
				assert.ErrorIs(t, tt.wantErr, err)
				return
			}

			assert.ElementsMatch(t, tt.wantOutputs, outputs)

			assert.Equal(t, len(tt.wantStages), len(stages))
			for i, expected := range tt.wantStages {
				assert.Equal(t, expected.Name, stages[i].Name)
				assert.Equal(t, expected.Type, stages[i].Type)
				assert.ElementsMatch(t, expected.Outputs, stages[i].Outputs)
			}
		})
	}
}
