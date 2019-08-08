package com.jpro;

import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoUtil {

    public static List<Document> extract(MongoCursor<Document> cursor) {
        List<Document> _r = new ArrayList<>();
        while (cursor.hasNext()) {
            Document ele = cursor.next();
            _r.add(ele);
        }
        return _r;
    }

}
