package test

import (
	"testing"

	eng "github.com/pickjunk/wuneng/engine"
	"github.com/pickjunk/wuneng/types"
)

func TestTenThousandsInitAndShutdown(t *testing.T) {
	for i := 0; i < 10000; i++ {
		var engine eng.Engine
		engine.Init(types.EngineInitOptions{
			SegmenterDictionaries: "./test_dict.txt",
			SynonymTokenFile:      "./test_synonym.txt",
		})

		docID := uint64(1)
		engine.IndexDocument(docID, types.DocumentIndexData{
			Content: "百度",
		}, false)
		docID++
		engine.IndexDocument(docID, types.DocumentIndexData{
			Content: "",
			Tokens: []types.TokenData{
				{Text: "包括我", Locations: []int{0}},
			},
		}, false)
		docID++
		engine.IndexDocument(docID, types.DocumentIndexData{
			Content: "baidu都是沙雕",
		}, false)
		engine.FlushIndex()

		engine.Shutdown()
	}
}
