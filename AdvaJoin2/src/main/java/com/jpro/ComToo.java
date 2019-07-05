package com.jpro;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Log4j2
public class ComToo {
    public static List<String> parseArrayString(String orig) {
        List<String> terms = new ArrayList<>();
        StringBuilder ss = new StringBuilder();
        // split
        boolean openCollect = false;
        for (int i = 0; i < orig.length(); i++) {
            if (orig.charAt(i) == '"') {
                if (openCollect) {
                    terms.add(ss.toString());
                    ss.delete(0, ss.length());
                }
                openCollect = !openCollect;
                continue;
            }
            if (openCollect) {
                ss.append(orig.charAt(i));
            }
        }
        return terms;
    }

    /**
     * CURD of database
     */
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
