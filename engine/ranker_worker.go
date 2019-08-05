package engine

import (
	"github.com/pickjunk/wuneng/types"
)

type rankerAddDocRequest struct {
	docID  uint64
	fields interface{}
}

type rankerRankRequest struct {
	docs                []types.IndexedDocument
	options             types.RankOptions
	rankerReturnChannel chan rankerReturnRequest
	countDocsOnly       bool
}

type rankerReturnRequest struct {
	docs    types.ScoredDocuments
	numDocs int
}

type rankerRemoveDocRequest struct {
	docID uint64
}

func (engine *Engine) rankerAddDocWorker(shard int) {
	for {
		select {
		case <-engine.shutdownChannel:
			return
		case request := <-engine.rankerAddDocChannels[shard]:
			engine.rankers[shard].AddDoc(request.docID, request.fields)
		}
	}
}

func (engine *Engine) rankerRankWorker(shard int) {
	for {
		select {
		case <-engine.shutdownChannel:
			return
		case request := <-engine.rankerRankChannels[shard]:
			if request.options.MaxOutputs != 0 {
				request.options.MaxOutputs += request.options.OutputOffset
			}
			request.options.OutputOffset = 0
			outputDocs, numDocs := engine.rankers[shard].Rank(request.docs, request.options, request.countDocsOnly)
			request.rankerReturnChannel <- rankerReturnRequest{docs: outputDocs, numDocs: numDocs}
		}
	}
}

func (engine *Engine) rankerRemoveDocWorker(shard int) {
	for {
		select {
		case <-engine.shutdownChannel:
			return
		case request := <-engine.rankerRemoveDocChannels[shard]:
			engine.rankers[shard].RemoveDoc(request.docID)
		}
	}
}
