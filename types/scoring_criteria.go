package types

// ScoringCriteria 评分规则通用接口
type ScoringCriteria interface {
	// 给一个文档评分，文档排序时先用第一个分值比较，如果
	// 分值相同则转移到第二个分值，以此类推。
	// 返回空切片表明该文档应该从最终排序结果中剔除。
	Score(doc IndexedDocument, fields interface{}) []float32
}

// RankByBM25 一个简单的评分规则，文档分数为BM25
type RankByBM25 struct {
}

// Score 获取文档评分
func (rule RankByBM25) Score(doc IndexedDocument, fields interface{}) []float32 {
	return []float32{doc.BM25}
}
