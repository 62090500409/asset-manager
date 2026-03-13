package assetmanager

import "time"

// KnowledgeSource is a struct that represents a knowledge source, which is input for indexing
type KnowledgeSource struct {
	// e.g. pdf, word, ppt, plain-text, image, audio, csv, excel, sql, mongodb
	// determined by which extractor is used, and it is used for mapping source to indexer.
	SourceType string
	// source name, generally, it is filename. but it can be other string for other types of sources.
	SourceName   string
	// file data read from source file
	SourceData *[]byte
	// file url, used for identifying the source link or for downloading the source.
	SourceUrl  *[]string
	// file content, generally, it is extracted from source file by extractor. e.g. by ocr or stt process.
	SourceContents *string
	// metadata, used for storing metadata of the source. (optional)
	Metadata *map[string]any
}

type AssetRef struct {
	KbId string
	AssetType string
	AssetId string
	RefType string
}

type ContextualAsset struct {
	IndexedBy string
	KbId string
	AssetType string
	AssetId string
	Version int
	Content string
	EmbeddingModel *string
	Refs *[]AssetRef
	Labels *[]string
	Metadata *map[string]any
}

type IndexingResult struct {
	KbId string
	Status bool
	IndexedAt time.Time

	Error *string

	Version *int
	EmbeddingModel *string
	EmbeddingDim *int
	AssetsCount *map[string]int
}

type RetrievedAsset struct {
	ContextualAsset
	Score *float64
}

type RetrieveResult struct {
	KbIds []string
	Query string
	Config *map[string]any

	Status bool
	RetrieveAt time.Time

	Error *string
	RetrievedAssets *[]RetrievedAsset
}