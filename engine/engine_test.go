package engine

import (
	"reflect"
	"testing"

	"github.com/pickjunk/wuneng/types"
	"github.com/pickjunk/wuneng/utils"
)

type ScoringFields struct {
	A, B, C float32
}

func AddDocs(engine *Engine) {
	docID := uint64(1)
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "中国有十三亿人口人口",
		Fields:  ScoringFields{1, 2, 3},
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "中国人口",
		Fields:  nil,
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "有人口",
		Fields:  ScoringFields{2, 3, 1},
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "有十三亿人口",
		Fields:  ScoringFields{2, 3, 3},
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "中国十三亿人口",
		Fields:  ScoringFields{0, 9, 1},
	}, false)
	engine.FlushIndex()
}

func addDocsWithLabels(engine *Engine) {
	docID := uint64(1)
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "此次百度收购将成中国互联网最大并购",
		Labels:  []string{"百度", "中国"},
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "百度宣布拟全资收购91无线业务",
		Labels:  []string{"百度"},
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "百度是中国最大的搜索引擎",
		Labels:  []string{"百度"},
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "百度在研制无人汽车",
		Labels:  []string{"百度"},
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "BAT是中国互联网三巨头",
		Labels:  []string{"百度"},
	}, false)
	engine.FlushIndex()
}

type RankByTokenProximity struct {
}

func (rule RankByTokenProximity) Score(
	doc types.IndexedDocument, fields interface{}) []float32 {
	if doc.TokenProximity < 0 {
		return []float32{}
	}
	return []float32{1.0 / (float32(doc.TokenProximity) + 1)}
}

func TestEngineIndexDocument(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		DefaultRankOptions: &types.RankOptions{
			OutputOffset:    0,
			MaxOutputs:      10,
			ScoringCriteria: &RankByTokenProximity{},
		},
		IndexerInitOptions: &types.IndexerInitOptions{
			IndexType: types.LocationsIndex,
		},
	})
	defer engine.Shutdown()

	AddDocs(&engine)

	outputs := engine.Search(types.SearchRequest{Text: "中国人口"})
	utils.Expect(t, "2", len(outputs.Tokens))
	utils.Expect(t, "中国", outputs.Tokens[0])
	utils.Expect(t, "人口", outputs.Tokens[1])
	utils.Expect(t, "3", len(outputs.Docs))

	utils.Expect(t, "2", outputs.Docs[0].DocID)
	utils.Expect(t, "1000", int(outputs.Docs[0].Scores[0]*1000))
	utils.Expect(t, "[0 6]", outputs.Docs[0].TokenSnippetLocations)

	utils.Expect(t, "5", outputs.Docs[1].DocID)
	utils.Expect(t, "100", int(outputs.Docs[1].Scores[0]*1000))
	utils.Expect(t, "[0 15]", outputs.Docs[1].TokenSnippetLocations)

	utils.Expect(t, "1", outputs.Docs[2].DocID)
	utils.Expect(t, "76", int(outputs.Docs[2].Scores[0]*1000))
	utils.Expect(t, "[0 18]", outputs.Docs[2].TokenSnippetLocations)
}

func TestReverseOrder(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		DefaultRankOptions: &types.RankOptions{
			ReverseOrder:    true,
			OutputOffset:    0,
			MaxOutputs:      10,
			ScoringCriteria: &RankByTokenProximity{},
		},
		IndexerInitOptions: &types.IndexerInitOptions{
			IndexType: types.LocationsIndex,
		},
	})
	defer engine.Shutdown()

	AddDocs(&engine)

	outputs := engine.Search(types.SearchRequest{Text: "中国人口"})
	utils.Expect(t, "3", len(outputs.Docs))

	utils.Expect(t, "1", outputs.Docs[0].DocID)
	utils.Expect(t, "5", outputs.Docs[1].DocID)
	utils.Expect(t, "2", outputs.Docs[2].DocID)
}

func TestOffsetAndMaxOutputs(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		DefaultRankOptions: &types.RankOptions{
			ReverseOrder:    true,
			OutputOffset:    1,
			MaxOutputs:      3,
			ScoringCriteria: &RankByTokenProximity{},
		},
		IndexerInitOptions: &types.IndexerInitOptions{
			IndexType: types.LocationsIndex,
		},
	})
	defer engine.Shutdown()

	AddDocs(&engine)

	outputs := engine.Search(types.SearchRequest{Text: "中国人口"})
	utils.Expect(t, "2", len(outputs.Docs))

	utils.Expect(t, "5", outputs.Docs[0].DocID)
	utils.Expect(t, "2", outputs.Docs[1].DocID)
}

type TestScoringCriteria struct {
}

func (criteria TestScoringCriteria) Score(
	doc types.IndexedDocument, fields interface{}) []float32 {
	if reflect.TypeOf(fields) != reflect.TypeOf(ScoringFields{}) {
		return []float32{}
	}
	fs := fields.(ScoringFields)
	return []float32{float32(doc.TokenProximity)*fs.A + fs.B*fs.C}
}

func TestSearchWithCriteria(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		DefaultRankOptions: &types.RankOptions{
			ScoringCriteria: TestScoringCriteria{},
		},
		IndexerInitOptions: &types.IndexerInitOptions{
			IndexType: types.LocationsIndex,
		},
	})
	defer engine.Shutdown()

	AddDocs(&engine)

	outputs := engine.Search(types.SearchRequest{Text: "中国人口"})
	utils.Expect(t, "2", len(outputs.Docs))

	utils.Expect(t, "1", outputs.Docs[0].DocID)
	utils.Expect(t, "18000", int(outputs.Docs[0].Scores[0]*1000))

	utils.Expect(t, "5", outputs.Docs[1].DocID)
	utils.Expect(t, "9000", int(outputs.Docs[1].Scores[0]*1000))
}

func TestCompactIndex(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		DefaultRankOptions: &types.RankOptions{
			ScoringCriteria: TestScoringCriteria{},
		},
	})
	defer engine.Shutdown()

	AddDocs(&engine)

	outputs := engine.Search(types.SearchRequest{Text: "中国人口"})
	utils.Expect(t, "2", len(outputs.Docs))

	utils.Expect(t, "5", outputs.Docs[0].DocID)
	utils.Expect(t, "9000", int(outputs.Docs[0].Scores[0]*1000))

	utils.Expect(t, "1", outputs.Docs[1].DocID)
	utils.Expect(t, "6000", int(outputs.Docs[1].Scores[0]*1000))
}

type BM25ScoringCriteria struct {
}

func (criteria BM25ScoringCriteria) Score(
	doc types.IndexedDocument, fields interface{}) []float32 {
	if reflect.TypeOf(fields) != reflect.TypeOf(ScoringFields{}) {
		return []float32{}
	}
	return []float32{doc.BM25}
}

func TestFrequenciesIndex(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		DefaultRankOptions: &types.RankOptions{
			ScoringCriteria: BM25ScoringCriteria{},
		},
		IndexerInitOptions: &types.IndexerInitOptions{
			IndexType: types.FrequenciesIndex,
		},
	})
	defer engine.Shutdown()

	AddDocs(&engine)

	outputs := engine.Search(types.SearchRequest{Text: "中国人口"})
	utils.Expect(t, "2", len(outputs.Docs))

	utils.Expect(t, "5", outputs.Docs[0].DocID)
	utils.Expect(t, "2311", int(outputs.Docs[0].Scores[0]*1000))

	utils.Expect(t, "1", outputs.Docs[1].DocID)
	utils.Expect(t, "2211", int(outputs.Docs[1].Scores[0]*1000))
}

func TestRemoveDocument(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		DefaultRankOptions: &types.RankOptions{
			ScoringCriteria: TestScoringCriteria{},
		},
	})
	defer engine.Shutdown()

	AddDocs(&engine)
	engine.RemoveDocument(5, false)
	engine.RemoveDocument(6, false)
	engine.FlushIndex()
	engine.IndexDocument(6, types.DocumentIndexData{
		Content: "中国人口有十三亿",
		Fields:  ScoringFields{0, 9, 1},
	}, false)
	engine.FlushIndex()

	outputs := engine.Search(types.SearchRequest{Text: "中国人口"})
	utils.Expect(t, "2", len(outputs.Docs))

	utils.Expect(t, "6", outputs.Docs[0].DocID)
	utils.Expect(t, "9000", int(outputs.Docs[0].Scores[0]*1000))
	utils.Expect(t, "1", outputs.Docs[1].DocID)
	utils.Expect(t, "6000", int(outputs.Docs[1].Scores[0]*1000))
}

func TestEngineIndexDocumentWithTokens(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		DefaultRankOptions: &types.RankOptions{
			OutputOffset:    0,
			MaxOutputs:      10,
			ScoringCriteria: &RankByTokenProximity{},
		},
		IndexerInitOptions: &types.IndexerInitOptions{
			IndexType: types.LocationsIndex,
		},
	})
	defer engine.Shutdown()

	docID := uint64(1)
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "",
		Tokens: []types.TokenData{
			{Text: "中国", Locations: []int{0}},
			{Text: "人口", Locations: []int{18, 24}},
		},
		Fields: ScoringFields{1, 2, 3},
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "",
		Tokens: []types.TokenData{
			{Text: "中国", Locations: []int{0}},
			{Text: "人口", Locations: []int{6}},
		},
		Fields: ScoringFields{1, 2, 3},
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "中国十三亿人口",
		Fields:  ScoringFields{0, 9, 1},
	}, false)
	engine.FlushIndex()

	outputs := engine.Search(types.SearchRequest{Text: "中国人口"})
	utils.Expect(t, "2", len(outputs.Tokens))
	utils.Expect(t, "中国", outputs.Tokens[0])
	utils.Expect(t, "人口", outputs.Tokens[1])
	utils.Expect(t, "3", len(outputs.Docs))

	utils.Expect(t, "2", outputs.Docs[0].DocID)
	utils.Expect(t, "1000", int(outputs.Docs[0].Scores[0]*1000))
	utils.Expect(t, "[0 6]", outputs.Docs[0].TokenSnippetLocations)

	utils.Expect(t, "3", outputs.Docs[1].DocID)
	utils.Expect(t, "100", int(outputs.Docs[1].Scores[0]*1000))
	utils.Expect(t, "[0 15]", outputs.Docs[1].TokenSnippetLocations)

	utils.Expect(t, "1", outputs.Docs[2].DocID)
	utils.Expect(t, "76", int(outputs.Docs[2].Scores[0]*1000))
	utils.Expect(t, "[0 18]", outputs.Docs[2].TokenSnippetLocations)
}

func TestEngineIndexDocumentWithContentAndLabels(t *testing.T) {
	var engine1, engine2 Engine
	engine1.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		IndexerInitOptions: &types.IndexerInitOptions{
			IndexType: types.LocationsIndex,
		},
	})
	defer engine1.Shutdown()
	engine2.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		IndexerInitOptions: &types.IndexerInitOptions{
			IndexType: types.DocIDsIndex,
		},
	})
	defer engine2.Shutdown()

	addDocsWithLabels(&engine1)
	addDocsWithLabels(&engine2)

	outputs1 := engine1.Search(types.SearchRequest{Text: "百度"})
	outputs2 := engine2.Search(types.SearchRequest{Text: "百度"})
	utils.Expect(t, "1", len(outputs1.Tokens))
	utils.Expect(t, "1", len(outputs2.Tokens))
	utils.Expect(t, "百度", outputs1.Tokens[0])
	utils.Expect(t, "百度", outputs2.Tokens[0])
	utils.Expect(t, "5", len(outputs1.Docs))
	utils.Expect(t, "5", len(outputs2.Docs))
}

func TestCountDocsOnly(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		DefaultRankOptions: &types.RankOptions{
			ReverseOrder:    true,
			OutputOffset:    0,
			MaxOutputs:      1,
			ScoringCriteria: &RankByTokenProximity{},
		},
		IndexerInitOptions: &types.IndexerInitOptions{
			IndexType: types.LocationsIndex,
		},
	})
	defer engine.Shutdown()

	AddDocs(&engine)
	engine.RemoveDocument(5, false)
	engine.FlushIndex()

	outputs := engine.Search(types.SearchRequest{Text: "中国人口", CountDocsOnly: true})
	utils.Expect(t, "0", len(outputs.Docs))
	utils.Expect(t, "2", len(outputs.Tokens))
	utils.Expect(t, "2", outputs.NumDocs)
}

func TestSearchWithin(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		DefaultRankOptions: &types.RankOptions{
			ReverseOrder:    true,
			OutputOffset:    0,
			MaxOutputs:      10,
			ScoringCriteria: &RankByTokenProximity{},
		},
		IndexerInitOptions: &types.IndexerInitOptions{
			IndexType: types.LocationsIndex,
		},
	})
	defer engine.Shutdown()

	AddDocs(&engine)

	docIDs := make(map[uint64]bool)
	docIDs[5] = true
	docIDs[1] = true
	outputs := engine.Search(types.SearchRequest{
		Text:   "中国人口",
		DocIDs: docIDs,
	})
	utils.Expect(t, "2", len(outputs.Tokens))
	utils.Expect(t, "中国", outputs.Tokens[0])
	utils.Expect(t, "人口", outputs.Tokens[1])
	utils.Expect(t, "2", len(outputs.Docs))

	utils.Expect(t, "1", outputs.Docs[0].DocID)
	utils.Expect(t, "76", int(outputs.Docs[0].Scores[0]*1000))
	utils.Expect(t, "[0 18]", outputs.Docs[0].TokenSnippetLocations)

	utils.Expect(t, "5", outputs.Docs[1].DocID)
	utils.Expect(t, "100", int(outputs.Docs[1].Scores[0]*1000))
	utils.Expect(t, "[0 15]", outputs.Docs[1].TokenSnippetLocations)
}

func TestEngineIndexDocumentWithSynonyms(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		SynonymTokenFile:      "../test/test_synonym.txt",
		DefaultRankOptions: &types.RankOptions{
			ScoringCriteria: TestScoringCriteria{},
		},
	})
	defer engine.Shutdown()

	docID := uint64(1)
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "百度",
		Fields:  ScoringFields{0, 9, 1},
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "",
		Tokens: []types.TokenData{
			{Text: "包括我", Locations: []int{0}},
		},
		Fields: ScoringFields{0, 9, 1},
	}, false)
	docID++
	engine.IndexDocument(docID, types.DocumentIndexData{
		Content: "baidu都是沙雕",
		Fields:  ScoringFields{0, 1, 1},
	}, false)
	engine.FlushIndex()

	outputs := engine.Search(types.SearchRequest{Text: "百度"})
	utils.Expect(t, "1", len(outputs.Tokens))
	utils.Expect(t, "百度", outputs.Tokens[0])

	utils.Expect(t, "2", len(outputs.Docs))
	utils.Expect(t, "1", outputs.Docs[0].DocID)
	utils.Expect(t, "3", outputs.Docs[1].DocID)

	outputs = engine.Search(types.SearchRequest{Text: "十三亿莆田广告"})
	utils.Expect(t, "3", len(outputs.Tokens))
	utils.Expect(t, "十三亿", outputs.Tokens[0])
	utils.Expect(t, "莆田", outputs.Tokens[1])
	utils.Expect(t, "广告", outputs.Tokens[2])

	utils.Expect(t, "1", len(outputs.Docs))
	utils.Expect(t, "3", outputs.Docs[0].DocID)
}

func TestEngineSegment(t *testing.T) {
	var engine Engine
	engine.Init(types.EngineInitOptions{
		SegmenterDictionaries: "../test/test_dict.txt",
		SynonymTokenFile:      "../test/test_synonym.txt",
		StopTokenFile:         "../test/test_stop.txt",
	})
	defer engine.Shutdown()

	outputs := engine.Segment("百度", false)
	utils.Expect(t, "1", len(outputs))
	utils.Expect(t, "百度", outputs[0])

	outputs = engine.Segment("十三亿莆田广告", false)
	utils.Expect(t, "3", len(outputs))
	utils.Expect(t, "十三亿", outputs[0])
	utils.Expect(t, "莆田", outputs[1])
	utils.Expect(t, "广告", outputs[2])

	outputs = engine.Segment("hello十三world", false)
	utils.Expect(t, "1", len(outputs))
	utils.Expect(t, "十三", outputs[0])

	outputs = engine.Segment("百度", true)
	utils.Expect(t, "4", len(outputs))
	utils.Expect(t, "百度", outputs[0])
	utils.Expect(t, "baidu", outputs[1])
	utils.Expect(t, "广告", outputs[2])
	utils.Expect(t, "莆田", outputs[3])

	outputs = engine.Segment("十三亿莆田广告", true)
	utils.Expect(t, "11", len(outputs))
	utils.Expect(t, "十三亿", outputs[0])
	utils.Expect(t, "都是沙雕", outputs[1])
	utils.Expect(t, "包括我", outputs[2])
	utils.Expect(t, "莆田", outputs[3])
	utils.Expect(t, "百度", outputs[4])
	utils.Expect(t, "baidu", outputs[5])
	utils.Expect(t, "广告", outputs[6])
	utils.Expect(t, "广告", outputs[7])
	utils.Expect(t, "百度", outputs[8])
	utils.Expect(t, "baidu", outputs[9])
	utils.Expect(t, "莆田", outputs[10])

	outputs = engine.Segment("hello十三world", true)
	utils.Expect(t, "1", len(outputs))
	utils.Expect(t, "十三", outputs[0])
}
