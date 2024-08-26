package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOutput_Summarized(t *testing.T) {
	tests := []struct {
		name   string
		output Output
		want   SummarizedOutput
	}{
		{
			name: "happy path",
			output: Output{
				Unit:   "unit",
				Status: OutputStatusError,
				Records: []Record{
					testRecord{"group1", "id1"},
					emptyGroup{GroupString("group2")},
				},
				Err: errTestMapper,
			},
			want: SummarizedOutput{
				Unit:        "unit",
				Status:      OutputStatusError,
				RecordCount: 1,
				GroupCount:  2,
				Err:         errTestMapper,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.output.Summarized())
		})
	}
}
