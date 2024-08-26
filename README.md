# go-pipeline package

## モチベーション

データのパイプライン処理においては、考慮が必要な多くの非機能要件が存在します。

- 並列制御（並列実行数、レートリミット）
- エラー処理（一部でのエラー or 全体でのエラー）
- メトリクス・ロギング

このパッケージは、単一プロセスで動くパイプライン処理を抽象化した軽量な Go ライブラリを提供することで、これらの非機能要件を実装しやすくし、開発者がコアロジックの実装に集中できるようにすることを目指すものです。

## MapReduce

このパッケージで実装されているパイプラインモデルは MapReduce という歴史あるプログラミングモデルを強く意識した設計となっています。前提知識としてこちらを参照しておくと理解が深まりやすいのではないかと思います。

https://www.talend.com/jp/resources/what-is-mapreduce/

## モデル

![346062370-f419de33-faac-41fc-9d2b-281cf84ed19c](https://github.com/user-attachments/assets/e6e769f5-6e8d-46a0-8309-ac3b2fe97a7b)

### パイプライン (Pipeline)

データの流れをモデル化したもの。各処理を行う複数のステージと、それらの間を流れるデータであるレコードから構成されます。
最初のステージに渡されるレコードは処理の開始を表す特殊な値であり、最後のステージから出力されたレコードがパイプライン全体の出力となります。

### レコード (Record)

パイプラインを流れるデータを抽象化したものです。レコードの区分けを表すグループ (Group) と、グループ内で一意の値を取る識別子 (Identifier) をヘッダーとして持ち、ペイロードとしてデータの実体を持ちます。

### ステージ (Stage)

`list(<group1, id1>) -> Stage() -> list(<group2, id2>)`

実際の計算処理を実行する部分です。各ステージはレコードの集合を受け取り、何らかの処理を行なって加工したデータをレコードの集合として返す必要があります。
MapReduce を参考に、現在 Mapper と Reducer という 2 つの処理アルゴリズムが実装されています。

#### マッパー (Mapper)

`<group1, id1> -> Mapper() -> list(<group2, id2>)`

1 つのレコードに対して複数のレコードを返す関数として表現され、それらをレコード単位で並列に複数実行することで全体の結果を生成します。主に外部 API からのデータの取得処理やデータの変換処理に利用することができます。

#### リデューサー (Reducer)

`<group1, list(id1)> -> Reduce() -> list(<group2, id2>)`

前段のレコードをグループ化し、各グループのレコードの集合に対して複数のレコードを返す関数として表現され、それらをグループ単位で並列に複数実行することで全体の結果を返します。主に後段の処理で加工されたデータを集約し集計や保存処理を行うために利用することができます。

### アウトプット (Output)

処理の単位ごとに出力されたレコードをまとめたものをレコードと区別してアウトプットと呼びます。アウトプットはレコードの他、処理の成功・失敗を表すステータス、エラーの場合にはエラーの情報も含みます。

ステータスがエラーになったアウトプットは以降のパイプラインからは除外され、成功したレコードのみで処理が進みます。発生したエラーは別途ステージの実行情報として集計されます。

## 使い方

`examples/` にサンプルコードを配置しているので参考にしてください。サンプルコードでは、VM に対する脆弱性スキャンを題材にリージョンの取得、インスタンスのリストアップ、スキャン、脆弱性の集計をモデル化しています。

### 1. 各ステージで扱う Record を定義する

Record は以下のようなインターフェースとして定義されています。

```go
type Record interface {
	Group() Group
	Identifier() string
}

type Group interface {
	String() string
}
```

- レコードのグループを表す値を `Record.Group()` として返すように実装します。Group は `String()` を持つ独自型として定義するか、Group にデータを持たせる必要がない場合は`GroupString(string)` を利用することができます。
- レコードの識別子を表す値を `Record.Identifier()` として返すように実装します。
- グループや識別子に値を持たせる必要がない場合は、 `GroupNA` や `IdentifierNA` を利用することができます。

<details>
<summary>サンプルコードの実装例</summary>

```go
type Region struct {
	Name string
}
// Implements Record
func (r *Region) Group() pipeline.Group { return pipeline.GroupNA }
func (r *Region) Identifier() string    { return r.Name }


type Instance struct {
	Region string
	ID     string
}
// Implements Record
func (i *Instance) Group() pipeline.Group { return pipeline.GroupString(i.Region) }
func (i *Instance) Identifier() string    { return i.ID }


type Vulnerability struct {
	Instance *Instance
	ID       string
}
// Implements Record
func (v *Vulnerability) Group() pipeline.Group { return pipeline.GroupString(v.ID) }
func (v *Vulnerability) Identifier() string    { return v.Instance.ID }


type VulnerabilityCount struct {
	VulnerabilityID string
	Count           int
}
// Implements Record
func (v *VulnerabilityCount) Group() pipeline.Group { return pipeline.GroupString(v.VulnerabilityID) }
func (v *VulnerabilityCount) Identifier() string    { return pipeline.IdentifierNA }
```

</details>

### 2. Mapper / Reducer を実装する

Mapper / Reducer はそれぞれ次のようなインターフェースとして定義されています。これらを満たす型を実装します。

input には前段で出力されたレコードが入ります。型アサーションにより型を特定した上で必要な情報を参照します。

```go
type Mapper interface {
	Map(ctx context.Context, input Record) ([]Record, error)
}

type Reducer interface {
	Reduce(ctx context.Context, group Group, inputs []Record) ([]Record, error)
}
```

<details>
<summary>サンプルコードの実装例</summary>

```go
type RegionLister struct{}

func (l *RegionLister) Map(ctx context.Context, input pipeline.Record) ([]pipeline.Record, error) {
	return []pipeline.Record{
		&Region{Name: "ap-northeast-1"},
		&Region{Name: "us-west-1"},
	}, nil
}

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

func (c *Counter) Reduce(ctx context.Context, group pipeline.Group, inputs []pipeline.Record) ([]pipeline.Record, error) {
    vulnerabilityID := group.String()

    return []pipeline.Record{
    	&VulnerabilityCount{VulnerabilityID: vulnerabilityID, Count: len(inputs)},
    }, nil

}
```

</details>

### 3. Pipeline を組み立てる

定義した Mapper や Reducer を使い、パイプラインを組み立てます。

```go
pp := pipeline.New(
    pipeline.MapStage("RegionLister", &RegionLister{}),
    pipeline.MapStage("VMLister", &VMLister{}, pipeline.StageTimeout(1*time.Second)),
    pipeline.MapStage("Scanner", &Scanner{}, pipeline.StageMaxParallel(3)),
    pipeline.ReduceStage("Counter", &Counter{}, pipeline.StageAbortIfAnyError(true)),
)
```

MapStage もしくは ReduceStage を使って、定義した Mapper や Reducer をパイプラインに組み込むことができます。
またオプション引数で以下の値を設定できます。

- `StageTimeout(d time.Duration)`: ステージ単位のタイムアウト。タイムアウト前に正常に完了したレコードは後続のステージに渡されそのまま実行されていきます。
- `StageMaxParallel(n int)`: 並列実行数の上限を指定します。Mapper の場合はレコード、Reducer の場合はグループの数が最大の並列数になります。
- `StageAbortIfAnyError(v bool)`: `true` に設定した場合、実行されているワーカーのいずれかでエラーが発生したらクリティカルなエラーとして全体の処理を中止します。データの保存など、失敗が許容されないクリティカルなステージに対して有効化してください。

### 4. Pipeline を実行する

`Execute(ctx context.Context)` で定義したパイプラインを実行します。

```go
outputs, stages, err := pp.Execute(context.Background())
```

`Execute()` の返り値は以下のようになります。

- `outputs []Record`: 最後のステージで処理が正常に完了したレコード
- `stages []StageExecution`: 各ステージでの実行結果。定義したステージ順に値が入る
- `err error`: `StageAbortIfAnyError` 設定時にエラーが発生した場合、全体のパイプラインを中止して該当エラーがここに入る

<details>
<summary>サンプルコードの実装例</summary>

```go
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
```

```sh
--- Outputs ---
VulnerabilityID: CVE-2020-5678, Count: 1
VulnerabilityID: CVE-2020-9012, Count: 1
--- Executions ---
Stage RegionLister: 2 records generated, 1 success, 0 errors
Stage VMLister: 2 records generated, 2 success, 0 errors
Stage Scanner: 2 records generated, 1 success, 1 errors
Stage Counter: 2 records generated, 2 success, 0 errors
```

</details>

### その他

- Reducer はデフォルトではレコードが 1 件以上存在するグループに対してしか処理が実行されません。レコードが 0 件のグループに対しても処理を走らせたい場合、 `EmptyGroup()` という特殊なレコードを前段のステージで出力することで Reducer にグループを認識させることができます。このレコードは、Mapper や Reducer のレコードとしては無視されます。

- 実行時に全ステージの channel を作成し、各ステージで完了した出力から後段に流していく実装となっているので、1 つのステージの実行が完了していない段階でも完了したレコードについて順次後段のステージの処理が実行されていきます。ただし、Reducer は全てのレコードの出力を待ち受けるため前段のステージ全体が完了してから実行されます。

## 参考実装

### pipeline/examples

脆弱性スキャンを題材にしたシンプルなユースケースをサンプルとして実装しています。 本 README と合わせて参照してください。

https://github.com/cloudbase-inc/go-pipeline/tree/main/examples

## ライセンス

このプロジェクトは MIT ライセンスの下でライセンスされています。詳細は LICENSE ファイルを参照してください。
