package com.jpro.proc;

import com.jpro.base.*;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Log4j2
public class CNCDataProcessor implements AbstractDataProcessor {

    private Properties context;

    private MongoClient mongo;

    public CNCDataProcessor(Properties p) {
        context = p;
        try {
            mongo = new MongoClient(context.getProperty("mongo.server.ip"),
                    Integer.parseInt(context.getProperty("mongo.server.port")));
        } catch (Exception e) {
            log.error("Mongo link failed - " + e);
        }
        CNCExceptionCollection = context.getProperty("mongo.CNC.exception.collection");

        processKey = context.getProperty("CNC.process.key");
        cellKey = context.getProperty("CNC.cell.key");
        machineKey = context.getProperty("CNC.machine.key");
        stageKey = context.getProperty("CNC.stage.key");
        stageValue = context.getProperty("CNC.stage.value");
    }

    @Override
    public Document parse(String in) {
        return Document.parse(in);
    }

    private String CNCExceptionCollection;
    @Override
    public void trap(String in) {
        Document row = new Document("raw", in);
        mongo.getDatabase(GlobalContext.mongoDatabase).getCollection(CNCExceptionCollection)
                .insertOne(row);
    }

    private static String processKey;
    private static String cellKey;
    private static String machineKey;
    private static String stageKey;
    @Override
    public void validate(Document in) {
        // TODO: validate minimum requirement
        if (in.containsKey(GlobalContext.uniqueIdKeyOfCnc)
                && in.get(GlobalContext.uniqueIdKeyOfCnc) instanceof String
                && in.containsKey(processKey)
                && in.get(processKey) instanceof String
                && in.containsKey(cellKey)
                && in.get(cellKey) instanceof String
                && in.containsKey(machineKey)
                && in.get(machineKey) instanceof String
                && in.containsKey(stageKey)
                && in.get(stageKey) instanceof String
        ) {

        } else {
            throw new RuntimeException("CNCDataProcessor::validate Not satisfy CNC data fields requirement.");
        }
    }

    private String stageValue;
    @Override
    public Document filter(Document in) {
        if (in.getString(stageKey).contains(stageValue)) {
            return in;
        }
        return null;
    }

    @Override
    public Document transform(Document in) {
        return in;
    }

    /**
     * Do store and notify in here
     */
    @Override
    public Document core(Document in) {
        String uniqueIDValue = in.getString(GlobalContext.uniqueIdKeyOfCnc);
        /// TODO: store origin data first
        MongoProxy.upsertToCnc(in);

        /// TODO: use bloom filter
        if (!GlobalBloom.getFilterForAIM().mightContain(uniqueIDValue)
                && !GlobalBloom.getFilterForNotJoin().mightContain(uniqueIDValue)) {
            return in;
        }

        /// TODO:
//        if (!GlobalBloom.getInstence().getFilterForAIM().mightContain(uniqueIDValue)) {
//
//        }

        /// TODO: process AIM data not joined
        MongoCursor<Document> joinedCursor = MongoProxy.queryFromAim(uniqueIDValue, context.getProperty("AIM.process.value1"), mongo);

        if (joinedCursor.hasNext()) {
            /// TODO: query all CNC data again & do update joined AIM data
           Document CNCData = query(uniqueIDValue, mongo);

            while (joinedCursor.hasNext()) {
                Document ele = joinedCursor.next();
                ele.put("cnc", CNCData.getList("cnc", Document.class));

                MongoProxy.upsertToAim(ele);
            }
        } else {
            MongoCursor<Document> unjoinedCursor = MongoProxy.queryFromUnjoined(uniqueIDValue, context.getProperty("AIM.process.value1"), mongo);

            if (unjoinedCursor.hasNext()) {
                /// TODO: collect AIM/CNC/SN data, do second join if necessary
                Document CNCData = query(uniqueIDValue, this.mongo);
                if (CNCData == null) {
                    log.error("Impossible !!! CNC cannot be empty, origin data - " + in.toJson());
                    return in;
                }

                while (unjoinedCursor.hasNext()) {
                    Document ele = unjoinedCursor.next();
                    ele.put("CNC", CNCData);
                    // TODO: validate the AIM data, if it has 'SN' & 'CNC', do second join
                    AIMDataProcessor.processSecondJoin(uniqueIDValue, ele, mongo);
                }
            }
        }

        return null;
    }

    @Override
    public void post(Document in) {

    }

    public KafkaConsumer<String, String> makeKafkaConsumer() {
        Properties conProps = KafkaProxy.makeBaseAttr();
        conProps.put("bootstrap.servers", context.getProperty("kafka.server.url"));
        conProps.put("group.id", context.getProperty("CNC.group.id"));
        KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(conProps);
        consumer.subscribe(Collections.singleton(context.getProperty("CNC.topic")));
        return consumer;
    }

    /**
     * New strategy of database accessing
     */
    public static Document query(String uniqueValue, MongoClient mongoClient) {
        MongoCursor<Document> dataOfCNC = MongoProxy.queryFromCnc(uniqueValue, mongoClient);
        if (!dataOfCNC.hasNext()) { return null; }
        Map<String, Document> procMap = new HashMap<>();
        while (dataOfCNC.hasNext()) {
            Document ele = dataOfCNC.next();
            if (ele.containsKey(processKey)
                    && ele.containsKey(cellKey)
                    && ele.containsKey(machineKey)) {
                String processValue = ele.getString(processKey);
                if (!procMap.containsKey(processValue)) {
                    procMap.put(processValue, ele);
                } else {
                    int time_sec = ele.getObjectId("_id").getTimestamp();
                    int time_rst = procMap.get(processValue).getObjectId("_id").getTimestamp();
                    if (time_sec > time_rst) {
                        procMap.put(processValue, ele);
                    }
                }
            }
        }
        List<Document> listOfCNC = new ArrayList<>();
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        procMap.forEach((k, v) -> {
            lock.writeLock().lock();
            listOfCNC.add(
                    new Document(processKey, v.getString(processKey))
                            .append(cellKey, v.getString(cellKey))
                            .append(machineKey, v.getString(machineKey)));
            lock.writeLock().unlock();
        });
        return new Document("cnc", listOfCNC);
    }

}
