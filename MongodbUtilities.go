package mongodbutilities

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Emulates a query builder object that encompasses a collection of query filters
type QuerySet struct {
	// Includes all AND-ed query filters
	Query []interface{}
	// Additional options for the Find() collection operation.
	FindOptions *options.FindOptions
	// Aditional options for the UpdateOne() and UpdateMany() collection operation.
	UpdateOptions *options.UpdateOptions
	// Additional options for the DeleteOne() and DeleteMany() collection operations.
	DeleteOptions *options.DeleteOptions
}

// Adds a new query filter, it will be AND-ed with the preceeding filters.
func (instance *QuerySet) Filter(queries ...interface{}) *QuerySet {
	instance.Query = append(instance.Query, queries...)

	return instance
}

// Adds an exclusion filter for the provided filters
func (instance *QuerySet) Exclude(queries ...interface{}) *QuerySet {
	instance.Query = append(instance.Query, bson.M{"$nor": queries})

	return instance
}

// Build the final filter to be passed to a retrieval operation
func (instance *QuerySet) Build() bson.M {
	query := bson.M{"$and": instance.Query}

	return query
}

// Initializes the additional options.(for Find, Update*, and Delete* operations)
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

// Sets the limit option for a Find operation
func (instance *QuerySet) Limit(limit int) *QuerySet {
	instance.initializeOptions()
	instance.FindOptions = instance.FindOptions.SetLimit(int64(limit))

	return instance
}

// Sets the sort options for a Finf operation
func (instance *QuerySet) Sort(sort interface{}) *QuerySet {
	instance.initializeOptions()
	instance.FindOptions = instance.FindOptions.SetSort(sort)

	return instance
}

// Sets the skip option for a Find operation.
func (instance *QuerySet) Skip(limit int) *QuerySet {
	instance.initializeOptions()
	instance.FindOptions = instance.FindOptions.SetSkip(int64(limit))

	return instance
}

// Initializes a QuerySet instance for an initial set of queries
func CreateQuery(queries ...interface{}) *QuerySet {
	var query QuerySet
	query.Filter(queries...)

	return &query
}

// Wrapper around the Skip() and Limit() methods. Emulates pagination.
func PaginateQuery(query *QuerySet, skip *int, limit *int) {
	if skip != nil {
		query.Skip(*skip)
	}

	if limit != nil {
		query.Limit(*limit)
	}
}

// Blueprint for a document that is to be stored in a collection.
type BaseModel interface {
	// Should be able to return the documents _id value
	GetID() primitive.ObjectID
	// Should be able to set the document's _id value.
	SetID(primitive.ObjectID)
}

// Inserts/ Updates the model(document) in a collection.
// Sets the _id value if its an insertion operation.
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

// Deletes the model(document) from a collection.
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

// Initializes a Mongodb database connection from a URI and a database name
func GetDatabase(url, name string) (*mongo.Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	clientOptions := options.Client().ApplyURI(url)
	client, err := mongo.Connect(ctx, clientOptions)

	if err != nil {
		return nil, err

	}

	return client.Database(name), nil
}

// Helper function for an InsertOne operation.
func InsertDocument(
	database *mongo.Database,
	collectionName string,
	document interface{},
) (*mongo.InsertOneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	collection := database.Collection(collectionName)
	res, err := collection.InsertOne(ctx, document)

	return res, err
}

// Helper function for an InsertMany operation.
func InsertDocuments(
	database *mongo.Database,
	collectionName string,
	document []interface{},
) (*mongo.InsertManyResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	collection := database.Collection(collectionName)
	res, err := collection.InsertMany(ctx, document)

	return res, err
}

// Helper function for a FindOne operation.
// Return no error in the case of no document found.
// Utilizes the QuerySet abstraction.
func GetDocument(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
) (*mongo.SingleResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	collection := database.Collection(collectionName)
	res := collection.FindOne(ctx, query.Build())

	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return nil, nil
		}

		return nil, res.Err()
	}

	return res, nil
}

// Helper function for a Find() operation.
// Utilizes the QuerySet abstraction.
func GetDocuments(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
) (*mongo.Cursor, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	collection := database.Collection(collectionName)

	if query.FindOptions != nil {
		return collection.Find(ctx, query.Build(), query.FindOptions)

	} else {
		return collection.Find(ctx, query.Build())
	}
}

// Helper function for an UpdateOne() operation.
// Utilizes the QuerySet abstraction.
func UpdateDocument(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
	update interface{},
) (*mongo.UpdateResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	collection := database.Collection(collectionName)
	res, err := collection.UpdateOne(ctx, query.Build(), update)

	return res, err
}

// Helper function for an UpdateMany() operation.
// Utilizes the QuerySet abstraction.
func UpdateDocuments(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
	update interface{},
) (*mongo.UpdateResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	collection := database.Collection(collectionName)
	res, err := collection.UpdateMany(ctx, query.Build(), update)

	return res, err
}

// Helper function for a DeleteOne() operation.
// Utilizes the QuerySet abstraction.
func DeleteDocument(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
) (*mongo.DeleteResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	collection := database.Collection(collectionName)
	res, err := collection.DeleteOne(ctx, query.Build())

	return res, err
}

// Helper function for a DeleteMany() operation.
// Utilizes the QuerySet abstraction.
func DeleteDocuments(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
) (*mongo.DeleteResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	collection := database.Collection(collectionName)
	res, err := collection.DeleteMany(ctx, query.Build())

	return res, err
}

// Helper function for a CountDocuments() operation.
// Utilizes the QuerySet abstraction.
func CountDocuments(
	database *mongo.Database,
	collectionName string,
	query *QuerySet,
) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	collection := database.Collection(collectionName)
	res, err := collection.CountDocuments(ctx, query.Build())

	return res, err
}

// Helper function for an Aggregate() operation.
func AggregateDocuments(
	database *mongo.Database,
	collectionName string,
	parameters interface{},
) (*mongo.Cursor, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	collection := database.Collection(collectionName)
	res, err := collection.Aggregate(ctx, parameters)

	return res, err
}

// Helper function for creating an index fo a single field
func CreateIndex(
	database *mongo.Database,
	collectionName string,
	field string,
	value int8,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	collection := database.Collection(collectionName)
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: field, Value: value},
		},
		Options: options.Index().SetUnique(true),
	}

	_, err := collection.Indexes().CreateOne(ctx, indexModel)

	return err
}

// Helper function for listing a database collections.
func ListCollections(database *mongo.Database) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)

	defer cancel()

	return database.ListCollectionNames(ctx, bson.M{})
}
