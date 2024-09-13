package pipeline

/* 利用者で実装すべきインターフェース */
type Record interface {
	Group() Group
	Identifier() string
}

type Group interface {
	String() string
}

/* パイプラインの開始点を表す特殊なレコード。処理関数内では無視すること */
type originInput struct{}

func (o originInput) Group() Group {
	return GroupString(na)
}

func (o originInput) Identifier() string {
	return na
}

/* その他ユーティリティ関数等 */
func RecordKey(r Record) string {
	return r.Group().String() + "/" + r.Identifier()
}

const na = "*"
const GroupNA = GroupString(na)
const IdentifierNA = na

// シンプルな文字列として表現されるグループ
type GroupString string

func (g GroupString) String() string {
	return string(g)
}

// 特定のグループのレコードが全て出力されたことを示すレコード
// Reducerのグループを確定させるために利用され、レコードとしては無視される
type groupCommit struct {
	group Group
}

func GroupCommit(g Group) groupCommit {
	return groupCommit{g}
}

func (g groupCommit) Group() Group {
	return g.group
}

func (g groupCommit) Identifier() string {
	return na
}

// Deprecated: 代わりにGroupCommitを利用してください
func EmptyGroup(g Group) groupCommit {
	return groupCommit{g}
}
