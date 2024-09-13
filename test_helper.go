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

type testReducer struct{}

var errTestReducer = fmt.Errorf("test reducer error")

// グループごとに件数を集計する
func (r *testReducer) Reduce(ctx context.Context, group Group, inputs []Record) ([]Record, error) {
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
		return []Record{}, nil
	}

	return []Record{
		testRecord{group.String(), fmt.Sprintf("%d", len(inputs))},
	}, nil
}

type testGenerator struct{}

// 適当に2つのレコードを生成する
func (g *testGenerator) Map(ctx context.Context, input Record) ([]Record, error) {
	return []Record{
		testRecord{"group1", "id1"},
		testRecord{"error", "id2"},
	}, nil
}

type testGeneratorTimeout struct{}

// 適当に2つのレコードを生成する
func (g *testGeneratorTimeout) Map(ctx context.Context, input Record) ([]Record, error) {
	return []Record{
		testRecord{"group1", "id1"},
		testRecord{"error", "id2"},
		testRecord{"timeout", "id3"},
		testRecord{"group4", "id4"},
	}, nil
}

type testBrokenGenerator struct{}

var errTestBrokenGenerator = errors.New("test broken generator error")

func (g *testBrokenGenerator) Map(ctx context.Context, input Record) ([]Record, error) {
	return nil, errTestBrokenGenerator
}
