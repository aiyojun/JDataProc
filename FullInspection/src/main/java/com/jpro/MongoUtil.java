package com.jpro;

import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoUtil {
    interface Extract<F, T> {
        F extract(T x);
    }

    public static   extract(MongoCursor<T> cursor, Extract e) {

    }

    public static List<Document> extract(MongoCursor<Document> cursor, f) {
        List<Document> _r = new ArrayList<>();
        while (cursor.hasNext()) {
            Document ele = cursor.next();
            f(ele)
//            _r.add(ele);
        }
        return _r;
    }
}
