package assetmanager

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoAssetManagerRepo struct {
	collection *mongo.Collection
}

func NewMongoAssetManagerRepo(ctx context.Context, collection *mongo.Collection) MongoAssetManagerRepo {
	return MongoAssetManagerRepo{collection: collection}
}

func (m *MongoAssetManagerRepo) Setup(ctx context.Context) error {
	cursor, err := m.collection.Indexes().List(ctx)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	var indexes []bson.D
	if err = cursor.All(ctx, &indexes); err != nil {
		return err
	}
	for _, index := range indexes {
		cursor.Decode(index)
		data, _ := bson.Marshal(index)
		var indexMap bson.M
		bson.Unmarshal(data, &indexMap)
		if indexMap["name"] == "asset_index" {
			return nil
		}
	}
	// create index
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "kbId", Value: 1},
			{Key: "assetType", Value: 1},
			{Key: "assetId", Value: 1},
			{Key: "version", Value: 1},
		},
		Options: options.Index().SetUnique(true).SetName("asset_index"),
	}
	_, err = m.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return err
	}
	return nil
}

func (m *MongoAssetManagerRepo) getLatestVersion(ctx context.Context, kbId string) (*int, error) {
	query := bson.M{"kbId": kbId}
	cursor, err := m.collection.Find(ctx, query)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var assets []ContextualAsset
	if err = cursor.All(ctx, &assets); err != nil {
		return nil, err
	}

	if len(assets) == 0 {
		return nil, nil
	}
	return &assets[0].Version, nil
}

func (m *MongoAssetManagerRepo) GetAssets(ctx context.Context, kbId string, assetType string, assetIds *[]string, version *int, label *string) ([]ContextualAsset, error) {
	query := bson.M{
		"kbId":      kbId,
		"assetType": assetType,
	}
	if assetIds != nil {
		query["assetId"] = bson.M{"$in": *assetIds}
	}

	if version != nil {
		query["version"] = *version
	} else {
		// get latest version
		latestVersion, err := m.getLatestVersion(ctx, kbId)
		if err != nil {
			return []ContextualAsset{}, err
		}
		if latestVersion != nil {
			query["version"] = *latestVersion
		} else {
			return []ContextualAsset{}, nil
		}
	}

	if label != nil {
		query["labels"] = *label
	}

	cursor, err := m.collection.Find(ctx, query)
	if err != nil {
		return []ContextualAsset{}, err
	}
	defer cursor.Close(ctx)
	var assets []ContextualAsset
	if err = cursor.All(ctx, &assets); err != nil {
		return []ContextualAsset{}, err
	}
	return assets, nil
}

func (m *MongoAssetManagerRepo) GetAssetsByKbId(ctx context.Context, kbId string) ([]ContextualAsset, error) {
	query := bson.M{"kbId": kbId}
	cursor, err := m.collection.Find(ctx, query)
	if err != nil {
		return []ContextualAsset{}, err
	}
	defer cursor.Close(ctx)
	var assets []ContextualAsset
	if err = cursor.All(ctx, &assets); err != nil {
		return []ContextualAsset{}, err
	}
	return assets, nil
}

func (m *MongoAssetManagerRepo) InsertAsset(ctx context.Context, asset ContextualAsset) (bool, error) {
	insertResult, err := m.collection.InsertOne(ctx, asset)
	if err != nil {
		return false, err
	}
	if insertResult != nil && insertResult.InsertedID != nil {
		return true, nil
	}
	return false, nil
}

func (m *MongoAssetManagerRepo) InsertBatchAssets(ctx context.Context, assets []ContextualAsset) (int, error) {
	// parse assets
	parsedAssets := make([]interface{}, len(assets))
	for i, asset := range assets {
		parsedAssets[i] = asset
	}
	insertResult, err := m.collection.InsertMany(ctx, parsedAssets)
	if err != nil {
		return 0, err
	}
	if insertResult != nil && len(insertResult.InsertedIDs) > 0 {
		return len(insertResult.InsertedIDs), nil
	}
	return 0, nil
}

func (m *MongoAssetManagerRepo) parseUpdateStringOperation(operation UpdateStringOperation, fieldName string, rootUpdate *bson.M) error {
	if strings.ToUpper(string(operation.Operation)) == "REPLACE" {
		if (*rootUpdate)["$set"] == nil {
			(*rootUpdate)["$set"] = bson.M{}
		}
		(*rootUpdate)["$set"].(bson.M)[fieldName] = operation.Value
	} else {
		return fmt.Errorf("invalid string operation: %s", operation.Operation)
	}
	return nil
}

func (m *MongoAssetManagerRepo) parseUpdateListOperation(operation UpdateListOperation, fieldName string, rootUpdate *bson.M) error {
	insertIndex := operation.Index
	if strings.ToUpper(string(operation.Operation)) == "INSERT" {
		if insertIndex == nil {
			if len(operation.Values) == 0 {
				return errors.New("invalid list operation")
			} else if len(operation.Values) == 1 {
				if (*rootUpdate)["$push"] == nil {
					(*rootUpdate)["$push"] = bson.M{}
				}
				(*rootUpdate)["$push"].(bson.M)[fieldName] = operation.Values[0]
			} else {
				if (*rootUpdate)["$push"] == nil {
					(*rootUpdate)["$push"] = bson.M{}
				}
				(*rootUpdate)["$push"].(bson.M)[fieldName] = bson.M{"$each": operation.Values}
			}
		} else {
			if len(operation.Values) == 0 {
				return errors.New("invalid list operation")
			} else {
				if (*rootUpdate)["$push"] == nil {
					(*rootUpdate)["$push"] = bson.M{}
				}
				(*rootUpdate)["$push"].(bson.M)[fieldName] = bson.M{"$each": operation.Values, "$slice": *insertIndex}
			}
		}
	} else if strings.ToUpper(string(operation.Operation)) == "REPLACE" {
		if (*rootUpdate)["$set"] == nil {
			(*rootUpdate)["$set"] = bson.M{}
		}
		(*rootUpdate)["$set"].(bson.M)[fieldName] = operation.Values
	} else if strings.ToUpper(string(operation.Operation)) == "REMOVE" {
		if len(operation.Values) == 0 {
			return errors.New("invalid list operation")
		} else if len(operation.Values) == 1 {
			if (*rootUpdate)["$pull"] == nil {
				(*rootUpdate)["$pull"] = bson.M{}
			}
			(*rootUpdate)["$pull"].(bson.M)[fieldName] = operation.Values[0]
		} else {
			if (*rootUpdate)["$pull"] == nil {
				(*rootUpdate)["$pull"] = bson.M{}
			}
			(*rootUpdate)["$pull"].(bson.M)[fieldName] = bson.M{"$in": operation.Values}
		}
	} else {
		return errors.New("invalid list operation")
	}
	return nil
}

func (m *MongoAssetManagerRepo) parseUpdateMapOperation(operation UpdateMapOperation, fieldName string, rootUpdate *bson.M) error {
	if strings.ToUpper(string(operation.Operation)) == "REPLACE" {
		if (*rootUpdate)["$set"] == nil {
			(*rootUpdate)["$set"] = bson.M{}
		}
		(*rootUpdate)["$set"].(bson.M)[fieldName] = bson.M(operation.Values)
	} else if strings.ToUpper(string(operation.Operation)) == "MERGE" {
		if (*rootUpdate)["$merge"] == nil {
			(*rootUpdate)["$merge"] = bson.M{}
		}
		(*rootUpdate)["$merge"].(bson.M)[fieldName] = bson.M(operation.Values)
	} else {
		return errors.New("invalid map operation")
	}
	return nil
}

func (m *MongoAssetManagerRepo) UpdateAsset(ctx context.Context, kbId string, assetType string, assetId string, version *int, updatedAsset UpdatedContextualAsset) (*ContextualAsset, error) {
	query := bson.M{
		"kbId":      kbId,
		"assetType": assetType,
		"assetId":   assetId,
	}
	if version != nil {
		query["version"] = *version
	} else {
		// get latest version
		latestVersion, err := m.getLatestVersion(ctx, kbId)
		if err != nil {
			return nil, err
		}
		if latestVersion != nil {
			query["version"] = *latestVersion
		} else {
			return nil, nil
		}
	}

	update := bson.M{}
	if updatedAsset.Content != nil {
		err := m.parseUpdateStringOperation(*updatedAsset.Content, "content", &update)
		if err != nil {
			return nil, err
		}
	}
	if updatedAsset.EmbeddingModel != nil {
		err := m.parseUpdateStringOperation(*updatedAsset.EmbeddingModel, "embeddingModel", &update)
		if err != nil {
			return nil, err
		}
	}
	if updatedAsset.Refs != nil {
		err := m.parseUpdateListOperation(*updatedAsset.Refs, "refs", &update)
		if err != nil {
			return nil, err
		}
	}
	if updatedAsset.Labels != nil {
		err := m.parseUpdateListOperation(*updatedAsset.Labels, "labels", &update)
		if err != nil {
			return nil, err
		}
	}
	if updatedAsset.Metadata != nil {
		err := m.parseUpdateMapOperation(*updatedAsset.Metadata, "metadata", &update)
		if err != nil {
			return nil, err
		}
	}

	updateResult, err := m.collection.UpdateOne(ctx, query, update)
	if err != nil {
		return nil, err
	}
	if updateResult != nil && updateResult.ModifiedCount > 0 {
		return &ContextualAsset{}, nil
	}
	return nil, nil
}

func (m *MongoAssetManagerRepo) DeleteAsset(ctx context.Context, kbId string, assetType string, assetId string, version *int) (bool, error) {
	query := bson.M{
		"kbId":      kbId,
		"assetType": assetType,
		"assetId":   assetId,
	}
	if version != nil {
		query["version"] = *version
	} else {
		// get latest version
		latestVersion, err := m.getLatestVersion(ctx, kbId)
		if err != nil {
			return false, err
		}
		if latestVersion != nil {
			query["version"] = *latestVersion
		} else {
			return false, nil
		}
	}

	deleteResult, err := m.collection.DeleteOne(ctx, query)
	if err != nil {
		return false, err
	}
	if deleteResult != nil && deleteResult.DeletedCount > 0 {
		return true, nil
	}
	return false, nil
}

func (m *MongoAssetManagerRepo) DeleteAssetsByKbId(ctx context.Context, kbId string) (int, error) {
	query := bson.M{"kbId": kbId}
	deleteResult, err := m.collection.DeleteMany(ctx, query)
	if err != nil {
		return 0, err
	}
	if deleteResult != nil && deleteResult.DeletedCount > 0 {
		return int(deleteResult.DeletedCount), nil
	}
	return 0, nil
}

func (m *MongoAssetManagerRepo) DeleteAssetsByKbIdAndAssetType(ctx context.Context, kbId string, assetType string) (int, error) {
	query := bson.M{
		"kbId":      kbId,
		"assetType": assetType,
	}
	deleteResult, err := m.collection.DeleteMany(ctx, query)
	if err != nil {
		return 0, err
	}
	if deleteResult != nil && deleteResult.DeletedCount > 0 {
		return int(deleteResult.DeletedCount), nil
	}
	return 0, nil
}
