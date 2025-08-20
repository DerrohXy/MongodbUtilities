package mongodbutilities

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type QuerySet struct {
	Query         []interface{}
	FindOptions   *options.FindOptions
	UpdateOptions *options.UpdateOptions
	DeleteOptions *options.DeleteOptions
}

func (instance *QuerySet) Filter(queries ...interface{}) *QuerySet {
	instance.Query = append(instance.Query, queries...)
	return instance
}

func (instance *QuerySet) Exclude(queries ...interface{}) *QuerySet {
	instance.Query = append(instance.Query, bson.M{"$nor": queries})
	return instance
}

func (instance *QuerySet) Build() bson.M {
	query := bson.M{"$and": instance.Query}
	return query
}

func (instance *QuerySet) initializeOptions() *QuerySet {
	if instance.FindOptions == nil {
		instance.FindOptions = options.Find()
	}
	if instance.UpdateOptions == nil {
		instance.UpdateOptions = options.Update()
	}
	if instance.DeleteOptions == nil {
		instance.DeleteOptions = options.Delete()
	}
	return instance
}

func (instance *QuerySet) Limit(limit int) *QuerySet {
	instance.initializeOptions()
	instance.FindOptions = instance.FindOptions.SetLimit(int64(limit))
	return instance
}

func (instance *QuerySet) Sort(sort interface{}) *QuerySet {
	instance.initializeOptions()
	instance.FindOptions = instance.FindOptions.SetSort(sort)
	return instance
}

func (instance *QuerySet) Skip(limit int) *QuerySet {
	instance.initializeOptions()
	instance.FindOptions = instance.FindOptions.SetSkip(int64(limit))
	return instance
}

func CreateQuery(queries ...interface{}) *QuerySet {
	var query QuerySet
	query.Filter(queries...)
	return &query
}

func PaginateQuery(query *QuerySet, skip *int, limit *int) {
	if skip != nil {
		query.Skip(*skip)
	}
	if limit != nil {
		query.Limit(*limit)
	}
}

//

type BaseModel interface {
	GetID() primitive.ObjectID
	SetID(primitive.ObjectID)
}

func SaveModel(instance BaseModel, database *mongo.Database, collectionName string) error {
	if instance.GetID() == primitive.NilObjectID {
		res, err := InsertDocument(database, collectionName, instance)
		if err == nil {
			instance.SetID(res.InsertedID.(primitive.ObjectID))
		}
		return err
	} else {
		var query QuerySet
		query.Filter(bson.M{"_id": instance.GetID()})
		_, err := UpdateDocument(
			database,
			collectionName,
			&query,
			bson.M{"$set": instance},
		)
		return err
	}
}

func DeleteModel(instance BaseModel, database *mongo.Database, collectionName string) error {
	if instance.GetID() == primitive.NilObjectID {
		return nil
	} else {
		var query QuerySet
		query.Filter(bson.M{"_id": instance.GetID()})
		_, err := DeleteDocument(
			database,
			collectionName,
			&query,
		)
		return err
	}
}

//

func GetDatabase(url, name string) (*mongo.Database, error) {
	clientOptions := options.Client().ApplyURI(url)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return nil, err
	}
	return client.Database(name), nil
}

func InsertDocument(
	database *mongo.Database,
	collectionName string,
	document interface{},
) (*mongo.InsertOneResult, error) {
	collection := database.Collection(collectionName)
	res, err := collection.InsertOne(context.TODO(), document)
	return res, err
}

func InsertDocuments(
	database *mongo.Database,
	collectionName string,
	document []interface{},
) (*mongo.InsertManyResult, error) {
	collection := database.Collection(collectionName)
	res, err := collection.InsertMany(context.TODO(), document)
	return res, err
}

func GetDocument(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
) (*mongo.SingleResult, error) {
	collection := database.Collection(collectionName)
	res := collection.FindOne(context.TODO(), query.Build())
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, res.Err()
	}
	return res, nil
}

func GetDocuments(
	database *mongo.Database,
	collectionName string,
	query *QuerySet) (*mongo.Cursor, error) {
	collection := database.Collection(collectionName)
	if query.FindOptions != nil {
		return collection.Find(context.TODO(), query.Build(), query.FindOptions)
	} else {
		return collection.Find(context.TODO(), query.Build())
	}
}

func UpdateDocument(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
	update interface{},
) (*mongo.UpdateResult, error) {
	collection := database.Collection(collectionName)
	res, err := collection.UpdateOne(context.TODO(), query.Build(), update)
	return res, err
}

func UpdateDocuments(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
	update interface{},
) (*mongo.UpdateResult, error) {
	collection := database.Collection(collectionName)
	res, err := collection.UpdateMany(context.TODO(), query.Build(), update)
	return res, err
}

func DeleteDocument(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
) (*mongo.DeleteResult, error) {
	collection := database.Collection(collectionName)
	res, err := collection.DeleteOne(context.TODO(), query.Build())
	return res, err
}

func DeleteDocuments(
	database *mongo.Database,
	collectionName string,
	query *QuerySet) (*mongo.DeleteResult, error) {
	collection := database.Collection(collectionName)
	res, err := collection.DeleteMany(context.TODO(), query.Build())
	return res, err
}

func CountDocuments(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
) (int64, error) {
	collection := database.Collection(collectionName)
	res, err := collection.CountDocuments(context.TODO(), query.Build())
	return res, err
}

func AggregateDocuments(
	database *mongo.Database,
	collectionName string,
	parameters interface{},
) (*mongo.Cursor, error) {
	collection := database.Collection(collectionName)
	res, err := collection.Aggregate(context.TODO(), parameters)
	return res, err
}

func ListCollections(database *mongo.Database) ([]string, error) {
	return database.ListCollectionNames(context.TODO(), bson.M{})
}
