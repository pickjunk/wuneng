package engine

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
)

// Synonym 同义词结构
type Synonym struct {
	text         string
	synonymGroup *SynonymGroup
}

// SynonymGroup 同义词组，组内单词平权，分词时会自动添加同义词到token中，逻辑搜索时会添加“or”逻辑
type SynonymGroup []*Synonym

// Synonyms 同义词列表
type Synonyms struct {
	Synonyms map[string]*Synonym
}

// String 字符串打印
func (sy *Synonyms) String() string {
	res, _ := json.Marshal(sy)
	return string(res)
}

// Init 初始化
func (sy *Synonyms) Init(synonymsTokenFile string) {
	if synonymsTokenFile == "" {
		return
	}

	file, err := os.Open(synonymsTokenFile)
	defer file.Close()
	if err != nil {
		log.Panic("Open stop token file error: ", err)
	}

	scanner := bufio.NewScanner(file)
	synonyms := make(map[string]*Synonym, 0)
	for scanner.Scan() {
		text := strings.ToLower(scanner.Text())
		words := strings.Split(text, " ")

		if len(words) > 0 {
			synonymGroup := SynonymGroup{}
			for _, w := range words {
				synonym := Synonym{
					text:         w,
					synonymGroup: &synonymGroup,
				}
				synonymGroup = append(synonymGroup, &synonym)
				synonyms[w] = &synonym
			}
		}
	}
	sy.Synonyms = synonyms
}

// GetSynonymsWords 获取同义词列表
func (sy *Synonyms) GetSynonymsWords(word string) []string {
	res := make([]string, 0)

	if synonym, ok := sy.Synonyms[word]; ok {
		for _, s := range *synonym.synonymGroup {
			res = append(res, s.text)
		}
	}

	return res
}
