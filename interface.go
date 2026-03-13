package assetmanager

import "context"

type UpdateStringOperation struct {
	// REPLACE
	Operation string
	Value     string
}

type UpdateListOperation struct {
	// INSERT, REPLACE, REMOVE
	Operation string
	Values    []any
	Index     *int
}

type UpdateMapOperation struct {
	// REPLACE, MERGE
	Operation string
	Values    map[string]any
}

type UpdatedContextualAsset struct {
	Content *UpdateStringOperation
	EmbeddingModel *UpdateStringOperation
	Refs *UpdateListOperation
	Labels *UpdateListOperation
	Metadata *UpdateMapOperation
}

type AssetManagerRepo interface {
	Setup(ctx context.Context) error
	GetAssets(ctx context.Context, kbId string, assetType string, assetIds *[]string, version *int, label *string) ([]ContextualAsset, error)
	GetAssetsByKbId(ctx context.Context, kbId string) ([]ContextualAsset, error)
	InsertAsset(ctx context.Context, asset ContextualAsset) (bool, error)
	InsertBatchAssets(ctx context.Context, assets []ContextualAsset) (int, error)
	UpdateAsset(ctx context.Context, kbId string, assetType string, assetId string, version *int, updatedAsset UpdatedContextualAsset) (*ContextualAsset, error)
	DeleteAsset(ctx context.Context, kbId string, assetType string, assetId string, version *int) (bool, error)
	DeleteAssetsByKbId(ctx context.Context, kbId string) (int, error)
	DeleteAssetsByKbIdAndAssetType(ctx context.Context, kbId string, assetType string) (int, error)
}