package com.jpro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import org.bson.Document;

import java.io.IOException;

import static com.mongodb.client.model.Filters.*;

public class ComToo {
    public static boolean askMongoHasKey(MongoClient mongoClient, String database, String coll, String key, String id) {
        return mongoClient.getDatabase(database).getCollection(coll).find(regex(key, id)).iterator().hasNext();
    }

    public static JsonNode parseJsonString(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(json);
    }

    public static void insert(MongoClient mongoClient, String database, String coll, Document doc) {
        mongoClient.getDatabase(database).getCollection(coll).insertOne(doc);
    }
}
