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

// ReducerでRecordがないグループも扱いたい場合に使うRecord
// Reducerのグループのリストを作成するために利用され、レコードとしては無視される
type emptyGroup struct {
	group Group
}

func EmptyGroup(g Group) emptyGroup {
	return emptyGroup{g}
}

func (g emptyGroup) Group() Group {
	return g.group
}

func (g emptyGroup) Identifier() string {
	return na
}
