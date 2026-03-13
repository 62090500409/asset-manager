package assetmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"go.mongodb.org/mongo-driver/bson"
)

type ElasticsearchAssetManagerRepo struct {
	assets_index string
	client *elasticsearch.Client
}

func NewElasticsearchAssetManagerRepo(
	ctx context.Context, 
	assets_index string, 
	client *elasticsearch.Client, 
	) ElasticsearchAssetManagerRepo {
	return ElasticsearchAssetManagerRepo{
		assets_index: assets_index,
		client: client,
	}
}

func (e *ElasticsearchAssetManagerRepo) Setup(ctx context.Context) error {
	// check if assets_index exists
	res, err := e.client.Indices.Exists([]string{e.assets_index}, e.client.Indices.Exists.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	
	if res.StatusCode == 404 {
		// create index
		mapping := map[string]interface{}{
			"mappings": map[string]interface{}{
				"properties": map[string]interface{}{
					"kbId": map[string]interface{}{"type": "keyword"},
					"assetType": map[string]interface{}{"type": "keyword"},
					"assetId": map[string]interface{}{"type": "keyword"},
					"version": map[string]interface{}{"type": "integer"},
					"content": map[string]interface{}{"type": "text"},
					"embeddingModel": map[string]interface{}{"type": "keyword"},
					"refs": map[string]interface{}{"type": "keyword"},
					"labels": map[string]interface{}{"type": "keyword"},
					"metadata": map[string]interface{}{"type": "object"},
					"createdAt": map[string]interface{}{"type": "date"},
					"updatedAt": map[string]interface{}{"type": "date"},
				},
			},
		}

		mappingBytes, err := json.Marshal(mapping)
		if err != nil {
			return err
		}

		res, err := e.client.Indices.Create(e.assets_index, e.client.Indices.Create.WithBody(bytes.NewReader(mappingBytes)))
		if err != nil {
			return err
		}
		defer res.Body.Close()
		
		if res.StatusCode != 200 {
			return fmt.Errorf("failed to create index %s because: %s", e.assets_index, res.String())
		}
	} else if res.StatusCode != 200 {
		return fmt.Errorf("index name %s not found", e.assets_index)
	}

	return nil	
}

func (e *ElasticsearchAssetManagerRepo) getLatestVersions(ctx context.Context, kbId string) (*int, error) {
	// Since all assets with same kbId must be same version, just get the max version for this kbId
	aggQuery := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{"term": map[string]interface{}{"kbId": kbId}},
				},
			},
		},
		"aggs": map[string]interface{}{
			"max_version": map[string]interface{}{
				"max": map[string]interface{}{
					"field": "version",
				},
			},
		},
		"size": 0, // Don't return actual documents, only aggregations
	}

	aggQueryBytes, err := json.Marshal(aggQuery)
	if err != nil {
		return nil, err
	}

	aggRes, err := e.client.Search(e.client.Search.WithIndex(e.assets_index), e.client.Search.WithBody(bytes.NewReader(aggQueryBytes)))
	if err != nil {
		return nil, err
	}
	defer aggRes.Body.Close()

	if aggRes.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get asset aggregations because: %s", aggRes.String())
	}

	var aggResult map[string]interface{}
	if err = json.NewDecoder(aggRes.Body).Decode(&aggResult); err != nil {
		return nil, err
	}
	
	// Extract the maximum version for this kbId
	maxVersionFloat := aggResult["aggregations"].(map[string]interface{})["max_version"].(map[string]interface{})["value"].(float64)
	maxVersion := int(maxVersionFloat)
	
	return &maxVersion, nil
}

func (e *ElasticsearchAssetManagerRepo) GetAssets(ctx context.Context, kbId string, assetType string, assetIds *[]string, version *int, label *string) ([]ContextualAsset, error) {
	// Build the base query
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{"term": map[string]interface{}{"kbId": kbId}},
				},
			},
		},
	}
	
	// Add version filter - if nil, get latest version; if specified, use that version
	if version != nil {
		query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = append(
			query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"].([]interface{}),
			map[string]interface{}{"term": map[string]interface{}{"version": *version}},
		)
	} else {
		// Get the latest version for this kbId
		latestVersion, err := e.getLatestVersions(ctx, kbId)
		if err != nil {
			return []ContextualAsset{}, err
		}
		
		if latestVersion == nil {
			return []ContextualAsset{}, nil
		}
		
		// Use the latest version from the aggregation
		query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = append(
			query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"].([]interface{}),
			map[string]interface{}{"term": map[string]interface{}{"version": *latestVersion}},
		)
	}
	
	// Only add assetType filter if it's not empty
	if assetType != "" {
		query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = append(
			query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"].([]interface{}),
			map[string]interface{}{"term": map[string]interface{}{"assetType": assetType}},
		)
	}

	if assetIds != nil {
		query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = append(
			query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"].([]interface{}),
			map[string]interface{}{"terms": map[string]interface{}{"assetId": *assetIds}},
		)
	}

	if label != nil {
		query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = append(
			query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"].([]interface{}),
			map[string]interface{}{"term": map[string]interface{}{"labels": *label}},
		)
	}

	queryBytes, err := json.Marshal(query)
	if err != nil {
		return []ContextualAsset{}, err
	}

	res, err := e.client.Search(e.client.Search.WithIndex(e.assets_index), e.client.Search.WithBody(bytes.NewReader(queryBytes)))
	if err != nil {
		return []ContextualAsset{}, err
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		var result map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
			return []ContextualAsset{}, err
		}
		
		var assets []ContextualAsset
		for _, hit := range result["hits"].(map[string]interface{})["hits"].([]interface{}) {
			source := hit.(map[string]interface{})["_source"].(map[string]interface{})
			
			// Handle optional fields
			var embeddingModel *string
			if em, ok := source["embeddingModel"].(string); ok {
				embeddingModel = &em
			}
			
			var refs *[]AssetRef
			if r, ok := source["refs"].([]interface{}); ok {
				var assetRefs []AssetRef
				for _, ref := range r {
					// Convert to AssetRef - assuming the structure matches
					if refMap, ok := ref.(map[string]interface{}); ok {
						assetRef := AssetRef{
							KbId: refMap["kbId"].(string),
							AssetType: refMap["assetType"].(string),
							AssetId: refMap["assetId"].(string),
							RefType: refMap["refType"].(string),
						}
						assetRefs = append(assetRefs, assetRef)
					}
				}
				refs = &assetRefs
			}
			
			var labels *[]string
			if l, ok := source["labels"].([]interface{}); ok {
				var stringLabels []string
				for _, label := range l {
					if strLabel, ok := label.(string); ok {
						stringLabels = append(stringLabels, strLabel)
					}
				}
				labels = &stringLabels
			}
			
			var metadata *map[string]any
			if m, ok := source["metadata"].(map[string]interface{}); ok {
				metadata = &m
			}
			
			asset := ContextualAsset{
				KbId: source["kbId"].(string),
				AssetType: source["assetType"].(string),
				AssetId: source["assetId"].(string),
				Version: int(source["version"].(float64)), // JSON numbers are float64
				Content: source["content"].(string),
				EmbeddingModel: embeddingModel,
				Refs: refs,
				Labels: labels,
				Metadata: metadata,
			}
			assets = append(assets, asset)
		}
		
		return assets, nil
	} else {
		return []ContextualAsset{}, fmt.Errorf("failed to get assets because: %s", res.String())
	}
}

func (e *ElasticsearchAssetManagerRepo) GetAssetsByKbId(ctx context.Context, kbId string) ([]ContextualAsset, error) {
	return e.GetAssets(ctx, kbId, "", nil, nil, nil)
}

func (e *ElasticsearchAssetManagerRepo) InsertAsset(ctx context.Context, asset ContextualAsset) (bool, error) {
	timestamp := time.Now().Format(time.RFC3339)
	assetMap := map[string]any{
		"kbId": asset.KbId,
		"assetType": asset.AssetType,
		"assetId": asset.AssetId,
		"version": asset.Version,
		"content": asset.Content,
		"embeddingModel": asset.EmbeddingModel,
		"refs": asset.Refs,
		"labels": asset.Labels,
		"metadata": asset.Metadata,
		"createdAt": timestamp,
		"updatedAt": timestamp,
	}
	data, err := json.Marshal(assetMap)
	if err != nil {
		return false, err
	}

	res, err := e.client.Index(e.assets_index, bytes.NewReader(data))
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.StatusCode == 201 {
		return true, nil
	} else {
		return false, fmt.Errorf("failed to insert asset because: %s", res.String())
	}
}

func (e *ElasticsearchAssetManagerRepo) getDocumentId(kbId string, assetType string, assetId string, version int) string {
	return fmt.Sprintf("%s:%s:%s:%d", kbId, assetType, assetId, version)
}

func (e *ElasticsearchAssetManagerRepo) InsertBatchAssets(ctx context.Context, assets []ContextualAsset) (int, error) {
	var buf bytes.Buffer
	timestamp := time.Now().Format(time.RFC3339)
	for _, asset := range assets {
		assetMap := map[string]any{
			"kbId": asset.KbId,
			"assetType": asset.AssetType,
			"assetId": asset.AssetId,
			"version": asset.Version,
			"content": asset.Content,
			"embeddingModel": asset.EmbeddingModel,
			"refs": asset.Refs,
			"labels": asset.Labels,
			"metadata": asset.Metadata,
			"createdAt": timestamp,
			"updatedAt": timestamp,
		}
		assetData, err := json.Marshal(assetMap)
		if err != nil {
			return 0, err
		}
		version := asset.Version
		docId := e.getDocumentId(asset.KbId, asset.AssetType, asset.AssetId, version)
		buf.WriteString(fmt.Sprintf(`{ "index" : { "_index" : "%s", "_id" : "%s", "_version" : %d } }\n%s\n`, e.assets_index, docId, version, string(assetData)))
	}

	res, err := e.client.Bulk(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		var result map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
			return 0, err
		}
		if result["errors"].(bool) {
			return 0, fmt.Errorf("failed to insert assets because: %s", res.String())
		}
		var successCount int
		for _, item := range result["items"].([]interface{}) {
			if item.(map[string]interface{})["index"].(map[string]interface{})["status"].(float64) == 201 {
				successCount++
			}
		}
		return successCount, nil
	} else {
		return 0, fmt.Errorf("failed to insert assets because: %s", res.String())
	}
}

func (e *ElasticsearchAssetManagerRepo) addPainlessScript(rootUpdate *bson.M, fieldName string, newScript string, newParams bson.M) (error) {
	if newParams[fieldName] == nil {
		return fmt.Errorf("field %s not found in newParams", fieldName)
	}
	if (*rootUpdate)["script"] == nil {
		(*rootUpdate)["script"] = bson.M{}
	}
	if (*rootUpdate)["script"].(bson.M)["source"] == nil {
		(*rootUpdate)["script"].(bson.M)["source"] = newScript
		(*rootUpdate)["script"].(bson.M)["params"] = newParams
	} else {
		(*rootUpdate)["script"].(bson.M)["source"] = fmt.Sprintf("%s\n%s", (*rootUpdate)["script"].(bson.M)["source"], newScript)
		(*rootUpdate)["script"].(bson.M)["params"].(bson.M)[fieldName] = newParams[fieldName]
	}
	return nil
}

func (e *ElasticsearchAssetManagerRepo) parseUpdateStringOperation(operation UpdateStringOperation, fieldName string, rootUpdate *bson.M) (error) {
	switch strings.ToUpper(operation.Operation) {
		case "REPLACE":
			newScript := fmt.Sprintf("ctx._source.%s = params.%s;", fieldName, fieldName)
			newParams := bson.M{fieldName: operation.Value}
			err := e.addPainlessScript(rootUpdate, fieldName, newScript, newParams)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported operation: %s", operation.Operation)
	}
	return nil
}

func (e *ElasticsearchAssetManagerRepo) parseUpdateListOperation(operation UpdateListOperation, fieldName string, rootUpdate *bson.M) (error) {
	switch strings.ToUpper(operation.Operation) {
		case "REPLACE":
			newScript := fmt.Sprintf("ctx._source.%s = params.%s;", fieldName, fieldName)
			newParams := bson.M{fieldName: operation.Values}
			err := e.addPainlessScript(rootUpdate, fieldName, newScript, newParams)
			if err != nil {
				return err
			}
		case "INSERT":
			if operation.Index == nil {
				newScript := fmt.Sprintf("ctx._source.%s.addAll(params.%s);", fieldName, fieldName)
				newParams := bson.M{fieldName: operation.Values}
				err := e.addPainlessScript(rootUpdate, fieldName, newScript, newParams)
				if err != nil {
					return err
				}
			} else {
				newScript := fmt.Sprintf("ctx._source.%s.add(params.%s.index, params.%s.value);", fieldName, fieldName, fieldName)
				newParams := bson.M{fieldName: bson.M{"index": *operation.Index, "value": operation.Values}}
				err := e.addPainlessScript(rootUpdate, fieldName, newScript, newParams)
				if err != nil {
					return err
				}
			}
		case "REMOVE":
			newScript := fmt.Sprintf("ctx._source.%s.removeAll(params.%s);", fieldName, fieldName)
			newParams := bson.M{fieldName: operation.Values}
			err := e.addPainlessScript(rootUpdate, fieldName, newScript, newParams)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported operation: %s", operation.Operation)
	}
	return nil
}

func (e *ElasticsearchAssetManagerRepo) parseUpdateMapOperation(operation UpdateMapOperation, fieldName string, rootUpdate *bson.M) (error) {
	switch strings.ToUpper(operation.Operation) {
		case "REPLACE":
			newScript := fmt.Sprintf("ctx._source.%s = params.%s;", fieldName, fieldName)
			newParams := bson.M{fieldName: bson.M(operation.Values)}
			err := e.addPainlessScript(rootUpdate, fieldName, newScript, newParams)
			if err != nil {
				return err
			}
		case "MERGE":
			newScript := fmt.Sprintf("ctx._source.%s.putAll(params.%s);", fieldName, fieldName)
			newParams := bson.M{fieldName: bson.M(operation.Values)}
			err := e.addPainlessScript(rootUpdate, fieldName, newScript, newParams)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported operation: %s", operation.Operation)
	}
	return nil
}

func (e *ElasticsearchAssetManagerRepo) UpdateAsset(ctx context.Context, kbId string, assetType string, assetId string, version *int, updatedAsset UpdatedContextualAsset) (*ContextualAsset, error) {
	if version == nil {
		latestVersion, err := e.getLatestVersions(ctx, kbId)
		if err != nil {
			return nil, err
		}
		version = latestVersion
	}
	rootUpdate := bson.M{}
	timestamp := time.Now().Format(time.RFC3339)
	e.addPainlessScript(&rootUpdate, "updatedAt", "ctx._source.updatedAt = params.updatedAt;", bson.M{"updatedAt": timestamp})
	if updatedAsset.Content != nil {
		e.parseUpdateStringOperation(*updatedAsset.Content, "content", &rootUpdate)
	}
	if updatedAsset.EmbeddingModel != nil {
		e.parseUpdateStringOperation(*updatedAsset.EmbeddingModel, "embeddingModel", &rootUpdate)
	}
	if updatedAsset.Refs != nil {
		e.parseUpdateListOperation(*updatedAsset.Refs, "refs", &rootUpdate)
	}
	if updatedAsset.Labels != nil {
		e.parseUpdateListOperation(*updatedAsset.Labels, "labels", &rootUpdate)
	}
	if updatedAsset.Metadata != nil {
		e.parseUpdateMapOperation(*updatedAsset.Metadata, "metadata", &rootUpdate)
	}
	docId := e.getDocumentId(kbId, assetType, assetId, *version)
	body, err := json.Marshal(rootUpdate)
	if err != nil {
		return nil, err
	}
	res, err := e.client.Update(e.assets_index, docId, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == 200 {
		var result map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
			return nil, err
		}
		if result["errors"].(bool) {
			return nil, fmt.Errorf("failed to update asset because: %s", res.String())
		}
		assets, err := e.GetAssets(ctx, kbId, assetType, &[]string{assetId}, version, nil)
		if err != nil {
			return nil, err
		}
		if len(assets) == 0 {
			return nil, fmt.Errorf("asset not found")
		}
		return &assets[0], nil
	} else {
		return nil, fmt.Errorf("failed to update asset because: %s", res.String())
	}
}

func (e *ElasticsearchAssetManagerRepo) DeleteAsset(ctx context.Context, kbId string, assetType string, assetId string, version *int) (error) {
	if version == nil {
		latestVersion, err := e.getLatestVersions(ctx, kbId)
		if err != nil {
			return err
		}
		version = latestVersion
	}
	docId := e.getDocumentId(kbId, assetType, assetId, *version)
	res, err := e.client.Delete(e.assets_index, docId)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == 200 {
		var result map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
			return err
		}
		if result["errors"].(bool) {
			return fmt.Errorf("failed to delete asset because: %s", res.String())
		}
		return nil
	} else {
		return fmt.Errorf("failed to delete asset because: %s", res.String())
	}
}

func (e *ElasticsearchAssetManagerRepo) DeleteAssetsByKbId(ctx context.Context, kbId string) (int, error) {
	query := bson.M{"query": bson.M{"term": bson.M{"kbId": kbId}}}
	queryJson, err := json.Marshal(query)
	if err != nil {
		return 0, err
	}
	res, err := e.client.DeleteByQuery([]string{e.assets_index}, bytes.NewReader(queryJson))
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode == 200 {
		var result map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
			return 0, err
		}
		if result["errors"].(bool) {
			return 0, fmt.Errorf("failed to delete assets because: %s", res.String())
		}
		return int(result["deleted"].(float64)), nil
	} else {
		return 0, fmt.Errorf("failed to delete assets because: %s", res.String())
	}
}
