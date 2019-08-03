package engine

// NumTokenIndexAdded 已加入检索队列的关键词数（只用于同步的计数器）
func (engine *Engine) NumTokenIndexAdded() uint64 {
	return engine.numTokenIndexAdded
}

// NumDocumentsIndexed 已加入检索队列的文档数（只用于同步的计数器）
func (engine *Engine) NumDocumentsIndexed() uint64 {
	return engine.numDocumentsIndexed
}

// NumDocumentsRemoved 已加入删除队列的文档数（只用于同步的计数器）
func (engine *Engine) NumDocumentsRemoved() uint64 {
	return engine.numDocumentsRemoved
}
