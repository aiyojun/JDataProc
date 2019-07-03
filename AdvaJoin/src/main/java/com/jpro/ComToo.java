package com.jpro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.log4j.Log4j2;
import lombok.var;
import org.bson.Document;

import javax.print.Doc;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

@Log4j2
public class ComToo {
    public static JsonNode parseJsonString(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(json);
    }

    public static Map<String, Object> parseJson(String json) {
        Map<String, Object> _r = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode root = mapper.readTree(json);
            for (var iter = root.fields(); iter.hasNext();) {
                var ele = iter.next();
                String   key = ele.getKey();
                JsonNode val = ele.getValue();
                if (val.isTextual()) {
                    _r.put(key, val.textValue());
                } else if (val.isNumber()) {
                    _r.put(key, val.numberValue());
                } else {
                    _r.put(key, val);
                }
            }
        } catch (IOException e) {
            log.error(e);
        }
        return _r;
    }

    /**
     * CURD of database
     */
    public static Map<String, Object> queryMongo(MongoClient mongoClient, String database, String col, String key, String val) {
        Map<String, Object> _r = new HashMap<>();
        FindIterable<Document> findIterable = mongoClient.getDatabase(database).getCollection(col).find(eq(key, val));
        findIterable.iterator().next().forEach(_r::put);
        return _r;
    }

    public static void updateMongo(MongoClient mongoClient, String database, String col, String key, String val, Document doc) {
        mongoClient.getDatabase(database).getCollection(col).findOneAndUpdate(eq(key, val), new Document("$set", doc));
    }

    public static void upsertMongo(MongoClient mongoClient, String database, String col, String key, String val, Document doc) {
        mongoClient.getDatabase(database).getCollection(col).replaceOne(eq(key, val), doc, new UpdateOptions().upsert(true));
    }

    public static Document findOneMongo(MongoClient mongoClient, String database, String col, String key, String val) {
        MongoCursor<Document> mongoCursor = mongoClient.getDatabase(database).getCollection(col).find(eq(key, val)).iterator();
        if (mongoCursor.hasNext()) {
            return mongoCursor.next();
        } else {
            return null;
        }
    }

    public static void insert(MongoClient mongoClient, String database, String coll, Document doc) {
        mongoClient.getDatabase(database).getCollection(coll).insertOne(doc);
    }
}
