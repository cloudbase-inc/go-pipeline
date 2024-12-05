package pipeline

import "testing"

func Test_isGroupCommit(t *testing.T) {
	type args struct {
		record Record
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "groupCommit struct",
			args: args{
				record: groupCommit{},
			},
			want: true,
		},
		{
			name: "GroupCommit interface (true)",
			args: args{
				record: testRecord{"group", ""},
			},
			want: true,
		},
		{
			name: "GroupCommit interface (false)",
			args: args{
				record: testRecord{"group", "id"},
			},
			want: false,
		},
		{
			name: "normal record",
			args: args{
				record: func() Record {
					var r Record
					return r
				}(),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isGroupCommit(tt.args.record); got != tt.want {
				t.Errorf("isGroupCommit() = %v, want %v", got, tt.want)
			}
		})
	}
}
