package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudbase-inc/go-pipeline"
)

/*
 * pipelineパッケージの使い方を説明するためのサンプルコード
 * AWSのEC2インスタンスに対する脆弱性スキャンを題材に、以下の処理をモデル化している
 *  - リージョン一覧取得
 *  - リージョンごとのVM一覧取得
 *  - VMごとの脆弱性スキャン
 *  - 脆弱性IDごとの集計
 */

type Region struct {
	Name string
}

// Implements Record
func (r *Region) Group() pipeline.Group { return pipeline.GroupNA }
func (r *Region) Identifier() string    { return r.Name }

type RegionLister struct{}

func (l *RegionLister) Map(ctx context.Context, input pipeline.Record) ([]pipeline.Record, error) {
	return []pipeline.Record{
		&Region{Name: "ap-northeast-1"},
		&Region{Name: "us-west-1"},
	}, nil
}

type Instance struct {
	Region string
	ID     string
}

// Implements Record
func (i *Instance) Group() pipeline.Group { return pipeline.GroupString(i.Region) }
func (i *Instance) Identifier() string    { return i.ID }

type VMLister struct{}

func (l *VMLister) Map(ctx context.Context, input pipeline.Record) ([]pipeline.Record, error) {
	region := input.(*Region)

	if region.Name == "ap-northeast-1" {
		return []pipeline.Record{
			&Instance{ID: "i-123"},
			&Instance{ID: "i-456"},
		}, nil
	}

	return nil, nil
}

type Vulnerability struct {
	Instance *Instance
	ID       string
}

// Implements Record
func (v *Vulnerability) Group() pipeline.Group { return pipeline.GroupString(v.ID) }
func (v *Vulnerability) Identifier() string    { return v.Instance.ID }

type Scanner struct{}

func (l *Scanner) Map(ctx context.Context, input pipeline.Record) ([]pipeline.Record, error) {
	instance := input.(*Instance)

	if instance.ID == "i-123" {
		return nil, fmt.Errorf("failed to scan instance %s", instance.ID)
	}
	if instance.ID == "i-456" {
		return []pipeline.Record{
			&Vulnerability{ID: "CVE-2020-5678"},
			&Vulnerability{ID: "CVE-2020-9012"},
		}, nil
	}

	return nil, nil
}

type Counter struct{}

type VulnerabilityCount struct {
	VulnerabilityID string
	Count           int
}

// Implements Record
func (v *VulnerabilityCount) Group() pipeline.Group { return pipeline.GroupString(v.VulnerabilityID) }
func (v *VulnerabilityCount) Identifier() string    { return pipeline.IdentifierNA }

func (c *Counter) Reduce(ctx context.Context, group pipeline.Group, inputs []pipeline.Record) ([]pipeline.Record, error) {
	vulnerabilityID := group.String()

	return []pipeline.Record{
		&VulnerabilityCount{VulnerabilityID: vulnerabilityID, Count: len(inputs)},
	}, nil
}

func main() {
	pp := pipeline.New(
		pipeline.MapStage("RegionLister", &RegionLister{}),
		pipeline.MapStage("VMLister", &VMLister{}, pipeline.StageTimeout(1*time.Second)),
		pipeline.MapStage("Scanner", &Scanner{}, pipeline.StageMaxParallel(3)),
		pipeline.ReduceStage("Counter", &Counter{}, pipeline.StageAbortIfAnyError(true)),
	)

	outputs, stages, err := pp.Execute(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Println("--- Outputs ---")
	for _, o := range outputs {
		v := o.(*VulnerabilityCount)
		fmt.Printf("VulnerabilityID: %s, Count: %d\n", v.VulnerabilityID, v.Count)
	}

	fmt.Println("--- Executions ---")
	for _, stage := range stages {
		successCount := 0
		errorCount := 0
		recordCount := 0
		for _, o := range stage.Outputs {
			if o.Status == pipeline.OutputStatusSuccess {
				successCount++
			}
			if o.Status == pipeline.OutputStatusError {
				errorCount++
			}
			recordCount += o.RecordCount
		}

		fmt.Printf("Stage %s: %d records generated, %d success, %d errors\n", stage.Name, recordCount, successCount, errorCount)
	}
}
