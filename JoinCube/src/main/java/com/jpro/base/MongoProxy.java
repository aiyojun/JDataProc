package com.jpro.base;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

@Log4j2
public class MongoProxy {
    private static MongoProxy instance;
    private MongoProxy() { }
    public static MongoProxy getInstance() {
        if (instance == null) {
            instance = new MongoProxy();
        }
        return instance;
    }

    private static Properties context;

    private static MongoClient linkOfAim;
    private static MongoClient linkOfCnc;
    private static MongoClient linkOfSn;
    public MongoProxy prepare() {
        context = GlobalContext.getInstance().getContext();
        linkOfAim = new MongoClient(context.getProperty("mongo.server.ip"),
                Integer.parseInt(context.getProperty("mongo.server.port")));
        linkOfCnc = new MongoClient(context.getProperty("mongo.server.ip"),
                Integer.parseInt(context.getProperty("mongo.server.port")));
        linkOfSn  = new MongoClient(context.getProperty("mongo.server.ip"),
                Integer.parseInt(context.getProperty("mongo.server.port")));
        return this;
    }
    public void close() {
        linkOfAim.close();
        linkOfCnc.close();
        linkOfSn.close();
    }

    private static ReentrantReadWriteLock lockOfAim = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock lockOfCnc = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock lockOfSn  = new ReentrantReadWriteLock();

    /**
     * AIM insert
     */
    public static void upsertToAim(String uniqueValue, String processValue, String stationValue, Document row) {
        String id = uniqueValue + processValue + stationValue;
        row.put("_id", id);
        GlobalBloom.getFilterForAIM().put(uniqueValue);
        lockOfAim.writeLock().lock();
        try {
            linkOfAim.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfAim)
                    .replaceOne(eq("_id", id), row, new UpdateOptions().upsert(true));
        } catch (Exception e) {

        }
        lockOfAim.writeLock().unlock();
        JComToo.statisticOfInsert.add(1);
    }

    public static void upsertToAim(Document row) {
        lockOfAim.writeLock().lock();
        try {
            linkOfAim.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfAim)
                    .replaceOne(eq("_id", row.getString("_id")), row, new UpdateOptions().upsert(true));
        } catch (Exception e) {

        }
        lockOfAim.writeLock().unlock();
        JComToo.statisticOfInsert.add(1);
    }

    public static MongoCursor<Document> queryFromAim(String uniqueValue, MongoClient mongoClient) {
        JComToo.statisticOfQuery.add(1);
        return mongoClient.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfAim)
                .find(eq(GlobalContext.uniqueIdKeyOfAim, uniqueValue)).iterator();
    }

    public static MongoCursor<Document> queryFromAim(String uniqueValue, String processValue, MongoClient mongoClient) {
        JComToo.statisticOfQuery.add(1);
        return mongoClient.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfAim)
                .find(and(eq(GlobalContext.uniqueIdKeyOfAim, uniqueValue),
                        eq(GlobalContext.processKeyOfAim, processValue))).iterator();
    }

    /**
     * Unjoined collection
     */
    public static void upsertToUnjoined(String uniqueValue, String processValue, String stationValue, Document row) {
        String id = uniqueValue + processValue + stationValue;
        row.put("_id", id);
        row.put(GlobalContext.uniqueIdKeyOfAim, uniqueValue);
        row.put(GlobalContext.processKeyOfAim, processValue);
        GlobalBloom.getFilterForNotJoin().put(uniqueValue);
        lockOfAim.writeLock().lock();
        try {
            linkOfAim.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfUnjoined)
                    .replaceOne(eq("_id", id), row, new UpdateOptions().upsert(true));
        } catch (Exception e) {

        }
        lockOfAim.writeLock().unlock();
        JComToo.statisticOfInsert.add(1);
    }

    public static void upsertToUnjoined(Document row) {
        lockOfAim.writeLock().lock();
        try {
            linkOfAim.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfUnjoined)
                    .replaceOne(eq("_id", row.getString("_id")), row, new UpdateOptions().upsert(true));
        } catch (Exception e) {

        }
        lockOfAim.writeLock().unlock();
        JComToo.statisticOfInsert.add(1);
    }

    public static MongoCursor<Document> queryFromUnjoined(String uniqueValue, String processValue, MongoClient mongoClient) {
        JComToo.statisticOfQuery.add(1);
        return mongoClient.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfUnjoined)
                .find(and(eq(GlobalContext.uniqueIdKeyOfAim, uniqueValue),
                        eq(GlobalContext.processKeyOfAim, processValue))).iterator();
    }

    public static MongoCursor<Document> queryFromUnjoined(String uniqueValue, MongoClient mongoClient) {
        JComToo.statisticOfQuery.add(1);
        return mongoClient.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfUnjoined)
                .find(eq(GlobalContext.uniqueIdKeyOfAim, uniqueValue)).iterator();
    }

    public static void upsertToCnc(Document row) {
        lockOfCnc.writeLock().lock();
        try {
            linkOfCnc.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfCnc)
                    .insertOne(row);
        } catch (Exception e) {

        }
        lockOfCnc.writeLock().unlock();
        JComToo.statisticOfInsert.add(1);
    }

    public static MongoCursor<Document> queryFromCnc(String uniqueValue, MongoClient mongoClient) {
        JComToo.statisticOfQuery.add(1);
        return mongoClient.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfCnc)
                .find(eq(GlobalContext.uniqueIdKeyOfCnc, uniqueValue)).iterator();
    }

    public static void upsertToSn(Document row) {
        lockOfSn.writeLock().lock();
        try {
            linkOfSn.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfSn)
                    .replaceOne(eq(GlobalContext.uniqueIdKeyOfSn, row.getString(GlobalContext.uniqueIdKeyOfSn)),
                            row, new UpdateOptions().upsert(true));
        } catch (Exception e) {

        }
        lockOfSn.writeLock().unlock();
        JComToo.statisticOfInsert.add(1);
    }

    public static Document queryFromSn(String uniqueValue, MongoClient mongoClient) {
        JComToo.statisticOfQuery.add(1);
        MongoCursor<Document> mongoCursor = mongoClient.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfSn)
                .find(eq(GlobalContext.uniqueIdKeyOfSn, uniqueValue)).iterator();
        if (mongoCursor.hasNext()) {
            Document _r = mongoCursor.next();
            _r.remove("_id");
            return _r;
        } else {
            return null;
        }
    }
}
