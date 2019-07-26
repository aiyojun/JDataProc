package com.jpro.base;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

@Log4j2
public class GlobalBloom {
    private static BloomFilter<String> filterForAIM;
    private static BloomFilter<String> filterForNotJoin;

//    private GlobalBloom() {
//        filterForAIM        = BloomFilter.create(Funnels.unencodedCharsFunnel(), Integer.parseInt(GlobalContext.context.getProperty("bloom.filter.AIM.size")), 0.01);
//        filterForNotJoin    = BloomFilter.create(Funnels.unencodedCharsFunnel(), Integer.parseInt(GlobalContext.context.getProperty("bloom.filter.unjoined.size")), 0.01);
//    }

//    private static GlobalBloom ins;
//    public static GlobalBloom getInstence() {
//        if (ins == null) {
//            ins = new GlobalBloom();
//        }
//        return ins;
//    }

    public static void loadBloomFilterCondition() {
        if (filterForAIM == null) {
            filterForAIM = BloomFilter.create(Funnels.unencodedCharsFunnel(), Integer.parseInt(GlobalContext.context.getProperty("bloom.filter.AIM.size")), 0.01);
        }
        if (filterForNotJoin == null) {
            filterForNotJoin = BloomFilter.create(Funnels.unencodedCharsFunnel(), Integer.parseInt(GlobalContext.context.getProperty("bloom.filter.unjoined.size")), 0.01);
        }

        MongoClient mongoClient = new MongoClient(GlobalContext.context.getProperty("mongo.server.ip"),
                Integer.parseInt(GlobalContext.context.getProperty("mongo.server.port")));
        FindIterable<Document> findIterable = null;
        MongoCursor<Document> mongoCursor = null;

        findIterable = mongoClient.getDatabase(GlobalContext.mongoDatabase)
                .getCollection(GlobalContext.collectionOfAim).find();
        mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()) {
            Document ele = mongoCursor.next();
            if (ele.containsKey(GlobalContext.uniqueIdKeyOfAim)) {
                filterForAIM.put(ele.getString(GlobalContext.uniqueIdKeyOfAim));
            }
        }

        findIterable = mongoClient.getDatabase(GlobalContext.mongoDatabase)
                .getCollection(GlobalContext.collectionOfUnjoined).find();
        mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()) {
            Document ele = mongoCursor.next();
            if (ele.containsKey(GlobalContext.uniqueIdKeyOfAim)) {
                filterForNotJoin.put(ele.getString(GlobalContext.uniqueIdKeyOfAim));
            }
        }
        log.info("\033[34;1m$$$$ \033[0mComplete load Bloom Filter data.");
        JComToo.sleep(2);
    }

    public static BloomFilter<String> getFilterForAIM() {
        if (filterForAIM == null) {
            filterForAIM = BloomFilter.create(Funnels.unencodedCharsFunnel(), Integer.parseInt(GlobalContext.context.getProperty("bloom.filter.AIM.size")), 0.01);
        }
        return filterForAIM;
    }

    public static BloomFilter<String> getFilterForNotJoin() {
        if (filterForNotJoin == null) {
            filterForNotJoin = BloomFilter.create(Funnels.unencodedCharsFunnel(), Integer.parseInt(GlobalContext.context.getProperty("bloom.filter.unjoined.size")), 0.01);
        }
        return filterForNotJoin;
    }
}
