package engine

import (
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cloudfoundry/bytefmt"
	"github.com/huichen/murmur"
	"github.com/huichen/sego"
	"github.com/pickjunk/wuneng/core"
	"github.com/pickjunk/wuneng/types"
	"github.com/pickjunk/wuneng/utils"
	"github.com/shirou/gopsutil/mem"
	"github.com/sirupsen/logrus"
)

const (
	numNanosecondsInAMillisecond = 1000000
)

// Engine struct
type Engine struct {
	// 计数器，用来统计有多少文档被索引等信息
	numDocumentsIndexed      uint64
	numDocumentsRemoved      uint64
	numDocumentsForceUpdated uint64
	numIndexingRequests      uint64
	numRemovingRequests      uint64
	numForceUpdatingRequests uint64
	numTokenIndexAdded       uint64

	// 记录初始化参数
	initOptions types.EngineInitOptions
	initialized bool

	indexers   []core.Indexer
	rankers    []core.Ranker
	segmenter  sego.Segmenter
	stopTokens StopTokens
	synonyms   *Synonyms

	// 建立索引器使用的通信通道
	segmenterChannel         chan segmenterRequest
	indexerAddDocChannels    []chan indexerAddDocumentRequest
	indexerRemoveDocChannels []chan indexerRemoveDocRequest
	rankerAddDocChannels     []chan rankerAddDocRequest

	// 建立排序器使用的通信通道
	indexerLookupChannels   []chan indexerLookupRequest
	rankerRankChannels      []chan rankerRankRequest
	rankerRemoveDocChannels []chan rankerRemoveDocRequest

	// 引擎退出的通信信道
	shutdownChannel chan bool
}

// Init 初始化搜索引擎，拉起所有worker
func (engine *Engine) Init(options types.EngineInitOptions) {
	// 将线程数设置为CPU数
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 初始化初始参数
	if engine.initialized {
		log.Panic("请勿重复初始化引擎")
	}
	options.Init()
	engine.initOptions = options
	engine.initialized = true

	if !options.NotUsingSegmenter {
		// 载入分词器词典
		engine.segmenter.LoadDictionary(options.SegmenterDictionaries)

		// 初始化停用词
		engine.stopTokens.Init(options.StopTokenFile)

		// 初始化同义词
		engine.synonyms = &Synonyms{}
		engine.synonyms.Init(options.SynonymTokenFile)
	}

	// 初始化索引器和排序器
	for shard := 0; shard < options.NumShards; shard++ {
		engine.indexers = append(engine.indexers, core.Indexer{})
		engine.indexers[shard].Init(*options.IndexerInitOptions)

		engine.rankers = append(engine.rankers, core.Ranker{})
		engine.rankers[shard].Init()
	}

	// 初始化分词器通道
	engine.segmenterChannel = make(
		chan segmenterRequest, options.NumSegmenterThreads)

	// 初始化索引器通道
	engine.indexerAddDocChannels = make(
		[]chan indexerAddDocumentRequest, options.NumShards)
	engine.indexerRemoveDocChannels = make(
		[]chan indexerRemoveDocRequest, options.NumShards)
	engine.indexerLookupChannels = make(
		[]chan indexerLookupRequest, options.NumShards)
	for shard := 0; shard < options.NumShards; shard++ {
		engine.indexerAddDocChannels[shard] = make(
			chan indexerAddDocumentRequest,
			options.IndexerBufferLength)
		engine.indexerRemoveDocChannels[shard] = make(
			chan indexerRemoveDocRequest,
			options.IndexerBufferLength)
		engine.indexerLookupChannels[shard] = make(
			chan indexerLookupRequest,
			options.IndexerBufferLength)
	}

	// 初始化排序器通道
	engine.rankerAddDocChannels = make(
		[]chan rankerAddDocRequest, options.NumShards)
	engine.rankerRankChannels = make(
		[]chan rankerRankRequest, options.NumShards)
	engine.rankerRemoveDocChannels = make(
		[]chan rankerRemoveDocRequest, options.NumShards)
	for shard := 0; shard < options.NumShards; shard++ {
		engine.rankerAddDocChannels[shard] = make(
			chan rankerAddDocRequest,
			options.RankerBufferLength)
		engine.rankerRankChannels[shard] = make(
			chan rankerRankRequest,
			options.RankerBufferLength)
		engine.rankerRemoveDocChannels[shard] = make(
			chan rankerRemoveDocRequest,
			options.RankerBufferLength)
	}

	// 初始化退出通道
	engine.shutdownChannel = make(
		chan bool, options.NumSegmenterThreads+6*options.NumShards)

	// 启动分词器
	for iThread := 0; iThread < options.NumSegmenterThreads; iThread++ {
		go engine.segmenterWorker()
	}

	// 启动索引器和排序器
	for shard := 0; shard < options.NumShards; shard++ {
		go engine.indexerAddDocumentWorker(shard)
		go engine.indexerRemoveDocWorker(shard)
		go engine.rankerAddDocWorker(shard)
		go engine.rankerRemoveDocWorker(shard)

		for i := 0; i < options.NumIndexerThreadsPerShard; i++ {
			go engine.indexerLookupWorker(shard)
		}
		for i := 0; i < options.NumRankerThreadsPerShard; i++ {
			go engine.rankerRankWorker(shard)
		}
	}
}

// Shutdown 中止所有worker，关闭引擎
func (engine *Engine) Shutdown() {
	options := engine.initOptions
	total := options.NumSegmenterThreads + 6*options.NumShards
	for i := 0; i < total; i++ {
		engine.shutdownChannel <- true
	}

	// 等待所有信号被消费完，即代表所有worker已退出
	for {
		runtime.Gosched()
		if len(engine.shutdownChannel) == 0 {
			return
		}
	}
}

// IndexDocument 将文档加入索引
//
// 输入参数：
//  docID	      标识文档编号，必须唯一，docID == 0 表示非法文档（用于强制刷新索引），[1, +oo) 表示合法文档
//  data	      见DocumentIndexData注释
//  forceUpdate 是否强制刷新 cache，如果设为 true，则尽快添加到索引，否则等待 cache 满之后一次全量添加
//
// 注意：
//      1. 这个函数是线程安全的，请尽可能并发调用以提高索引速度
//      2. 这个函数调用是非同步的，也就是说在函数返回时有可能文档还没有加入索引中，因此
//         如果立刻调用Search可能无法查询到这个文档。强制刷新索引请调用FlushIndex函数。
func (engine *Engine) IndexDocument(docID uint64, data types.DocumentIndexData, forceUpdate bool) {
	engine.internalIndexDocument(docID, data, forceUpdate)
}

func (engine *Engine) internalIndexDocument(
	docID uint64, data types.DocumentIndexData, forceUpdate bool) {
	if !engine.initialized {
		log.Panic("必须先初始化引擎")
	}

	if docID != 0 {
		atomic.AddUint64(&engine.numIndexingRequests, 1)
	}
	if forceUpdate {
		atomic.AddUint64(&engine.numForceUpdatingRequests, 1)
	}
	hash := murmur.Murmur3([]byte(fmt.Sprintf("%d%s", docID, data.Content)))
	engine.segmenterChannel <- segmenterRequest{
		docID: docID, hash: hash, data: data, forceUpdate: forceUpdate}
}

// RemoveDocument 将文档从索引中删除
//
// 输入参数：
//  docID	      标识文档编号，必须唯一，docID == 0 表示非法文档（用于强制刷新索引），[1, +oo) 表示合法文档
//  forceUpdate 是否强制刷新 cache，如果设为 true，则尽快删除索引，否则等待 cache 满之后一次全量删除
//
// 注意：
//      1. 这个函数是线程安全的，请尽可能并发调用以提高索引速度
//      2. 这个函数调用是非同步的，也就是说在函数返回时有可能文档还没有加入索引中，因此
//         如果立刻调用Search可能无法查询到这个文档。强制刷新索引请调用FlushIndex函数。
func (engine *Engine) RemoveDocument(docID uint64, forceUpdate bool) {
	if !engine.initialized {
		log.Panic("必须先初始化引擎")
	}

	if docID != 0 {
		atomic.AddUint64(&engine.numRemovingRequests, 1)
	}
	if forceUpdate {
		atomic.AddUint64(&engine.numForceUpdatingRequests, 1)
	}
	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		engine.indexerRemoveDocChannels[shard] <- indexerRemoveDocRequest{docID: docID, forceUpdate: forceUpdate}
		if docID == 0 {
			continue
		}
		engine.rankerRemoveDocChannels[shard] <- rankerRemoveDocRequest{docID: docID}
	}
}

// Search 查找满足搜索条件的文档，此函数线程安全
func (engine *Engine) Search(request types.SearchRequest) (output types.SearchResponse) {
	if !engine.initialized {
		log.Panic("必须先初始化引擎")
	}

	var rankOptions types.RankOptions
	if request.RankOptions == nil {
		rankOptions = *engine.initOptions.DefaultRankOptions
	} else {
		rankOptions = *request.RankOptions
	}
	if rankOptions.ScoringCriteria == nil {
		rankOptions.ScoringCriteria = engine.initOptions.DefaultRankOptions.ScoringCriteria
	}

	// 收集关键词
	tokens := []string{}
	if request.Text != "" {
		querySegments := engine.segmenter.Segment([]byte(request.Text))
		for _, s := range querySegments {
			token := s.Token().Text()
			if !engine.stopTokens.IsStopToken(token) {
				tokens = append(tokens, s.Token().Text())
			}
		}
	} else {
		for _, t := range request.Tokens {
			tokens = append(tokens, t)
		}
	}

	// 建立排序器返回的通信通道
	rankerReturnChannel := make(
		chan rankerReturnRequest, engine.initOptions.NumShards)

	// 生成查找请求
	lookupRequest := indexerLookupRequest{
		countDocsOnly:       request.CountDocsOnly,
		tokens:              tokens,
		labels:              request.Labels,
		docIDs:              request.DocIDs,
		options:             rankOptions,
		rankerReturnChannel: rankerReturnChannel,
		orderless:           request.Orderless,
	}

	// 向索引器发送查找请求
	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		engine.indexerLookupChannels[shard] <- lookupRequest
	}

	// 从通信通道读取排序器的输出
	numDocs := 0
	rankOutput := types.ScoredDocuments{}
	timeout := request.Timeout
	isTimeout := false
	if timeout <= 0 {
		// 不设置超时
		for shard := 0; shard < engine.initOptions.NumShards; shard++ {
			rankerOutput := <-rankerReturnChannel
			if !request.CountDocsOnly {
				for _, doc := range rankerOutput.docs {
					rankOutput = append(rankOutput, doc)
				}
			}
			numDocs += rankerOutput.numDocs
		}
	} else {
		// 设置超时
		deadline := time.Now().Add(time.Nanosecond * time.Duration(numNanosecondsInAMillisecond*request.Timeout))
		for shard := 0; shard < engine.initOptions.NumShards; shard++ {
			select {
			case rankerOutput := <-rankerReturnChannel:
				if !request.CountDocsOnly {
					for _, doc := range rankerOutput.docs {
						rankOutput = append(rankOutput, doc)
					}
				}
				numDocs += rankerOutput.numDocs
			case <-time.After(deadline.Sub(time.Now())):
				isTimeout = true
				break
			}
		}
	}

	// 再排序
	if !request.CountDocsOnly && !request.Orderless {
		if rankOptions.ReverseOrder {
			sort.Sort(sort.Reverse(rankOutput))
		} else {
			sort.Sort(rankOutput)
		}
	}

	// 准备输出
	output.Tokens = tokens
	// 仅当CountDocsOnly为false时才充填output.Docs
	if !request.CountDocsOnly {
		if request.Orderless {
			// 无序状态无需对Offset截断
			output.Docs = rankOutput
		} else {
			var start, end int
			if rankOptions.MaxOutputs == 0 {
				start = utils.MinInt(rankOptions.OutputOffset, len(rankOutput))
				end = len(rankOutput)
			} else {
				start = utils.MinInt(rankOptions.OutputOffset, len(rankOutput))
				end = utils.MinInt(start+rankOptions.MaxOutputs, len(rankOutput))
			}
			output.Docs = rankOutput[start:end]
		}
	}
	output.NumDocs = numDocs
	output.Timeout = isTimeout
	return
}

// MemoryUsage 打印内存使用率
func (engine *Engine) MemoryUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	v, _ := mem.VirtualMemory()

	log.WithFields(logrus.Fields{
		"heap":       bytefmt.ByteSize(m.Alloc),
		"sys":        bytefmt.ByteSize(m.Sys),
		"usage":      fmt.Sprintf("%.2f%%", float64(m.Sys)/float64(v.Total)),
		"totalUsage": fmt.Sprintf("%.2f%%", v.UsedPercent),
	}).Info("memory usage")
}

// FlushIndex 阻塞等待直到所有索引添加完毕
func (engine *Engine) FlushIndex() {
	for {
		runtime.Gosched()
		if engine.numIndexingRequests == engine.numDocumentsIndexed &&
			engine.numRemovingRequests*uint64(engine.initOptions.NumShards) == engine.numDocumentsRemoved {
			// 保证 CHANNEL 中 REQUESTS 全部被执行完
			break
		}
	}
	// 强制更新，保证其为最后的请求
	engine.IndexDocument(0, types.DocumentIndexData{}, true)
	for {
		runtime.Gosched()
		if engine.numForceUpdatingRequests*uint64(engine.initOptions.NumShards) == engine.numDocumentsForceUpdated {
			engine.MemoryUsage()
			return
		}
	}
}

// 从文本hash得到要分配到的shard
func (engine *Engine) getShard(hash uint32) int {
	return int(hash - hash/uint32(engine.initOptions.NumShards)*uint32(engine.initOptions.NumShards))
}
