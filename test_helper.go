package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type testMapper struct {
}

type testRecord struct {
	group      string
	identifier string
}

func (r testRecord) Group() Group {
	return GroupString(r.group)
}

func (r testRecord) Identifier() string {
	return r.identifier
}

var errTestMapper = errors.New("test mapper error")

// 適当にsuffixつけて倍増させる
func (m *testMapper) Map(ctx context.Context, input Record) ([]Record, error) {
	gr := input.Group().String()

	if strings.Contains(gr, "group1") {
		return []Record{
			testRecord{gr + "_mapped", input.Identifier() + "_1"},
			testRecord{gr + "_mapped", input.Identifier() + "_2"},
			GroupCommit(GroupString(gr + "_empty")),
		}, nil
	}
	// Error
	if strings.Contains(gr, "error") {
		return nil, errTestMapper
	}
	// Timeout
	if strings.Contains(gr, "timeout") {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
		}
		return []Record{}, nil
	}
	return nil, nil
}

type testMapperAbort struct{}

func (m *testMapperAbort) Map(ctx context.Context, input testRecord) ([]Record, error) {
	outputs, err := (&testMapper{}).Map(ctx, input)
	if err != nil {
		return nil, AbortError(err)
	}
	return outputs, nil
}

type testReducer struct{}

var errTestReducer = fmt.Errorf("test reducer error")

// グループごとに件数を集計する
func (r *testReducer) Reduce(ctx context.Context, group Group, inputs []testRecord) ([]testRecord, error) {
	// Error
	if strings.Contains(group.String(), "error") {
		return nil, errTestReducer
	}
	// Timeout
	if strings.Contains(group.String(), "timeout") {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
		}
		return []testRecord{}, nil
	}

	return []testRecord{
		{group.String(), fmt.Sprintf("%d", len(inputs))},
	}, nil
}

type testGenerator struct{}

// 適当に2つのレコードを生成する
func (g *testGenerator) Map(ctx context.Context, input Origin) ([]testRecord, error) {
	return []testRecord{
		{"group1", "id1"},
		{"error", "id2"},
	}, nil
}

type testReducerAbort struct{}

func (m *testReducerAbort) Reduce(ctx context.Context, group Group, inputs []testRecord) ([]testRecord, error) {
	outputs, err := (&testReducer{}).Reduce(ctx, group, inputs)
	if err != nil {
		return nil, AbortError(err)
	}
	return outputs, nil
}

type testGeneratorTimeout struct{}

// 適当に2つのレコードを生成する
func (g *testGeneratorTimeout) Map(ctx context.Context, input Origin) ([]testRecord, error) {
	return []testRecord{
		{"group1", "id1"},
		{"error", "id2"},
		{"timeout", "id3"},
		{"group4", "id4"},
	}, nil
}

type testBrokenGenerator struct{}

var errTestBrokenGenerator = errors.New("test broken generator error")

func (g *testBrokenGenerator) Map(ctx context.Context, input Origin) ([]testRecord, error) {
	return nil, AbortError(errTestBrokenGenerator)
}

type testStreamer struct{}

func (s *testStreamer) Stream(ctx context.Context, inputs <-chan Record) (<-chan Record, <-chan error) {
	outs := make(chan Record)
	errs := make(chan error)

	go func() {
		defer close(outs)
		defer close(errs)

		// errorを1件発生させる
		errs <- errors.New("something wrong")

		// inputをそのまま流す
		for {
			select {
			case <-ctx.Done():
				return
			case input, ok := <-inputs:
				if !ok {
					return
				}
				outs <- input
			}
		}
	}()

	return outs, errs
}

var errStreamFatal = errors.New("fatal stream error")

type testStreamerFatalError struct{}

func (s *testStreamerFatalError) Stream(ctx context.Context, inputs <-chan Record) (<-chan Record, <-chan error) {
	outs := make(chan Record)
	errs := make(chan error)

	go func() {
		defer close(outs)
		defer close(errs)

		// クリティカルなエラーを返す
		errs <- AbortError(errStreamFatal)
	}()

	return outs, errs
}

type testStreamerTimeout struct{}

func (s *testStreamerTimeout) Stream(ctx context.Context, inputs <-chan Record) (<-chan Record, <-chan error) {
	outs := make(chan Record)
	errs := make(chan error)

	go func() {
		defer close(outs)
		defer close(errs)

		<-time.After(10 * time.Second)
	}()

	return outs, errs
}
