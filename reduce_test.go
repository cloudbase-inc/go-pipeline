package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_reducerProcessor_Process(t *testing.T) {
	type args struct {
		ctx    context.Context
		inputs []Record
	}
	tests := []struct {
		name    string
		reducer *reduceProcessor
		args    args
		want    []Output
		wantErr error
	}{
		{
			name:    "happy path",
			reducer: newReduceProcessor("test", &testReducer{}),
			args: args{
				ctx: context.Background(),
				inputs: []Record{
					testRecord{"group1", "id1"},
					testRecord{"group1", "id2"},
					GroupCommit(GroupString("group1")),
					testRecord{"group2", "id3"}, // ASSERT: GroupCommitされていないグループも正しく処理される
					testRecord{"error", "id4"},
					GroupCommit(GroupString("group3")),
					GroupCommit(GroupString("group3")), // ASSERT: 複数回同じGroupCommitが来ても無視される
					testRecord{"group1", "id5"},        // ASSERT: GroupCommit後に流れてきたレコードは無視される
				},
			},
			want: []Output{
				{
					Unit:   "group1",
					Status: OutputStatusSuccess,
					Records: []Record{
						testRecord{"group1", "2"},
					},
				},
				{
					Unit:   "group2",
					Status: OutputStatusSuccess,
					Records: []Record{
						testRecord{"group2", "1"},
					},
				},
				{
					Unit:   "error",
					Status: OutputStatusError,
					Err:    errTestReducer,
				},
				{
					Unit:   "group3",
					Status: OutputStatusSuccess,
					Records: []Record{
						testRecord{"group3", "0"},
					},
				},
			},
		},
		{
			name: "timeout",
			reducer: func() *reduceProcessor {
				pr := newReduceProcessor("test", &testReducer{})
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
					testRecord{"timeout1", "id1"},
					testRecord{"timeout2", "id2"},
				},
			},
			want: []Output{
				{
					Unit:   "timeout1",
					Status: OutputStatusError,
					Err:    context.DeadlineExceeded,
				},
				{
					Unit:   "timeout2",
					Status: OutputStatusError,
					Err:    context.DeadlineExceeded,
				},
			},
		},
		{
			name: "abort",
			reducer: func() *reduceProcessor {
				pr := newReduceProcessor("test", &testReducer{})
				pr.SetAbortIfAnyError(true)
				return pr
			}(),
			args: args{
				ctx: context.Background(),
				inputs: []Record{
					testRecord{"group1", "id1"},
					testRecord{"group2", "id2"},
					testRecord{"error", "id3"},
				},
			},
			wantErr: errTestReducer,
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
			for o := range tt.reducer.Process(tt.args.ctx, inputs, abort) {
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
