package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_mapProcessor_Process(t *testing.T) {
	type args struct {
		ctx    context.Context
		inputs []Record
	}
	tests := []struct {
		name    string
		mapper  *mapProcessor
		args    args
		want    []Output
		wantErr error
	}{
		{
			name:   "happy path",
			mapper: newMapProcessor("test", &testMapper{}),
			args: args{
				ctx: context.Background(),
				inputs: []Record{
					testRecord{"group1", "id1"},
					testRecord{"error", "id2"},
					emptyGroup{GroupString("group2")},
				},
			},
			want: []Output{
				{
					Unit:   "group1/id1",
					Status: OutputStatusSuccess,
					Records: []Record{
						testRecord{"group1_mapped", "id1_1"},
						testRecord{"group1_mapped", "id1_2"},
						emptyGroup{GroupString("group1_empty")},
					},
				},
				{
					Unit:   "error/id2",
					Status: OutputStatusError,
					Err:    errTestMapper,
				},
			},
		},
		{
			name: "timeout",
			mapper: func() *mapProcessor {
				pr := newMapProcessor("test", &testMapper{})
				pr.SetMaxParallel(1)
				return pr
			}(),
			args: args{
				ctx: func() context.Context {
					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					t.Cleanup(cancel)
					return ctx
				}(),
				inputs: []Record{
					testRecord{"group1", "id1"},
					testRecord{"timeout", "id2"},
					testRecord{"error", "id3"},
				},
			},
			want: []Output{
				{
					Unit:   "group1/id1",
					Status: OutputStatusSuccess,
					Records: []Record{
						testRecord{"group1_mapped", "id1_1"},
						testRecord{"group1_mapped", "id1_2"},
						emptyGroup{GroupString("group1_empty")},
					},
				},
				// 順番に処理されるので、timeout以降のものは全てtimeoutとして処理される
				{
					Unit:   "timeout/id2",
					Status: OutputStatusError,
					Err:    context.DeadlineExceeded,
				},
				{
					Unit:   "error/id3",
					Status: OutputStatusError,
					Err:    context.DeadlineExceeded,
				},
			},
		},
		{
			name: "abort",
			mapper: func() *mapProcessor {
				pr := newMapProcessor("test", &testMapper{})
				pr.SetAbortIfAnyError(true)
				return pr
			}(),
			args: args{
				ctx: context.Background(),
				inputs: []Record{
					testRecord{"group1", "id1"},
					testRecord{"error", "id3"},
				},
			},
			wantErr: errTestMapper,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputs := make(chan Record)
			go func() {
				for _, in := range tt.args.inputs {
					inputs <- in
				}
				close(inputs)
			}()

			abort := make(chan error, 1)

			outputs := []Output{}
			for o := range tt.mapper.Process(tt.args.ctx, inputs, abort) {
				outputs = append(outputs, o)
			}

			close(abort)
			if tt.wantErr != nil {
				assert.ErrorIs(t, tt.wantErr, <-abort)
				return
			}

			assert.ElementsMatch(t, tt.want, outputs)
		})
	}
}
