package pipeline

import "context"

type Processor interface {
	Name() string
	Type() ProcessorType

	// list(<group1, id1>) -> Stage() -> list(<group2, id2>)
	Process(ctx context.Context, inputs <-chan Record, abort chan<- error) <-chan Output

	// Options
	SetMaxParallel(max int)
	SetAbortIfAnyError(value bool)
}

type ProcessorType string

const (
	ProcessorTypeMap    ProcessorType = "Map"
	ProcessorTypeReduce ProcessorType = "Reduce"
)

type OutputStatus string

const (
	OutputStatusSuccess OutputStatus = "Success"
	OutputStatusError   OutputStatus = "Error"
)

type Output struct {
	Unit    string
	Status  OutputStatus
	Records []Record
	Err     error
}

type SummarizedOutput struct {
	Unit        string
	Status      OutputStatus
	RecordCount int
	GroupCount  int
	Err         error
}

func (o Output) Summarized() SummarizedOutput {
	groups := map[string]struct{}{}
	var recordCount int
	for _, r := range o.Records {
		groups[r.Group().String()] = struct{}{}
		if _, ok := r.(emptyGroup); !ok {
			recordCount++
		}
	}

	return SummarizedOutput{
		Unit:        o.Unit,
		Status:      o.Status,
		RecordCount: recordCount,
		GroupCount:  len(groups),
		Err:         o.Err,
	}
}
