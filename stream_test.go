package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_streamProcessor_Process(t *testing.T) {
	type args struct {
		ctx    context.Context
		inputs []Record
	}
	tests := []struct {
		name     string
		streamer *streamProcessor
		args     args
		want     []Output
		wantErr  error
	}{
		{
			name:     "happy path",
			streamer: newStreamProcessor("test", &testStreamer{}),
			args: args{
				ctx: context.Background(),
				inputs: []Record{
					testRecord{"group1", "id1"},
					testRecord{"group1", "id2"},
					GroupCommit(GroupString("group1")),
					testRecord{"group2", "id3"},
				},
			},
			want: []Output{
				{
					Status: OutputStatusError,
					Err:    errors.New("something wrong"),
				},
				{
					Status:  OutputStatusSuccess,
					Records: []Record{testRecord{"group1", "id1"}},
				},
				{
					Status:  OutputStatusSuccess,
					Records: []Record{testRecord{"group1", "id2"}},
				},
				{
					Status:  OutputStatusSuccess,
					Records: []Record{GroupCommit(GroupString("group1"))},
				},
				{
					Status:  OutputStatusSuccess,
					Records: []Record{testRecord{"group2", "id3"}},
				},
			},
		},
		{
			name:     "timeout",
			streamer: newStreamProcessor("test", &testStreamerTimeout{}),
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
			wantErr: context.DeadlineExceeded,
		},
		{
			name:     "abort",
			streamer: newStreamProcessor("test", &testStreamerFatalError{}),
			args: args{
				ctx: context.Background(),
				inputs: []Record{
					testRecord{"group1", "id1"},
					testRecord{"group2", "id2"},
					testRecord{"error", "id3"},
				},
			},
			wantErr: errStreamFatal,
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
			for o := range tt.streamer.Process(tt.args.ctx, inputs, abort) {
				outputs = append(outputs, o)
			}

			close(abort)
			if tt.wantErr != nil {
				assert.ErrorIs(t, <-abort, tt.wantErr)
				return
			}

			assert.ElementsMatch(t, tt.want, outputs)
		})
	}
}
