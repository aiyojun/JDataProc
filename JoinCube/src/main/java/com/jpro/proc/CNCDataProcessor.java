package com.jpro.proc;

import com.jpro.base.JComToo;
import com.jpro.base.KafkaProxy;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;

@Log4j2
public class CNCDataProcessor implements AbstractDataProcessor {

    private Properties context;

    private MongoClient mongo;

    private String mongoDatabaseName;

    public CNCDataProcessor(Properties p) {
        context = p;
//        JComToo.log("\033[34;1m$$$$\033[0m CNC -- MongoDB link --");
        try {
            mongo = new MongoClient(context.getProperty("mongo.server.ip"),
                    Integer.parseInt(context.getProperty("mongo.server.port")));
        } catch (Exception e) {
            log.error("Mongo link failed - " + e);
        }
//        mongo = new MongoClient(context.getProperty("mongo.server.ip"),
//                Integer.parseInt(context.getProperty("mongo.server.port")));
        mongoDatabaseName = context.getProperty("mongo.database");
        CNCCollection = context.getProperty("mongo.CNC.collection");
        CNCExceptionCollection = context.getProperty("mongo.CNC.exception.collection");
        AIMCollection = context.getProperty("mongo.AIM.collection");
        AIMNotJoinedCollection = context.getProperty("mongo.AIM.unjoined.collection");

        uniqueIDKey = context.getProperty("CNC.unique.key");
        processKey = context.getProperty("CNC.process.key");
        cellKey = context.getProperty("CNC.cell.key");
        machineKey = context.getProperty("CNC.machine.key");
        stageKey = context.getProperty("CNC.stage.key");
        stageValue = context.getProperty("CNC.stage.value");

        AIMProcessValueOfCNC = context.getProperty("AIM.process.value1");

//        JComToo.log("CNCDataProcessor Constructor Over");
    }

    @Override
    public Document parse(String in) {
//        log.info("----- CNC -----");
        return Document.parse(in);
    }

    private String CNCExceptionCollection;
    @Override
    public void trap(String in) {
        Document row = new Document("raw", in);
        mongo.getDatabase(mongoDatabaseName).getCollection(CNCExceptionCollection).insertOne(row);
    }

    private String uniqueIDKey;
    private String processKey;
    private String cellKey;
    private String machineKey;
    private String stageKey;
    @Override
    public void validate(Document in) {
        // TODO: validate minimum requirement
//        log.info("CNCDataProcessor ---- validate----1--");
        if (in.containsKey(uniqueIDKey)
                && in.get(uniqueIDKey) instanceof String
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
//        log.info("CNCDataProcessor ---- filter----1--");
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
    private String AIMCollection;
    private String AIMNotJoinedCollection;
    private String AIMProcessValueOfCNC;
    @Override
    public Document core(Document in) {
//        log.info("CNCDataProcessor ---- core  2--");
        String uniqueIDValue = in.getString(uniqueIDKey);
        String processValue = in.getString(processKey);
        String id = uniqueIDValue + processValue;
        /// TODO: store origin data first
        store(id, in);

        /// TODO: process AIM data not joined
        MongoCursor<Document> joinedCursor = mongo.getDatabase(mongoDatabaseName).getCollection(AIMCollection)
                .find(regex("_id", uniqueIDValue + AIMProcessValueOfCNC + "*")).iterator();
        if (joinedCursor.hasNext()) {
            /// TODO: query all CNC data again & do update joined AIM data
           Document CNCData = getCNCData(uniqueIDValue);

            while (joinedCursor.hasNext()) {
                Document ele = joinedCursor.next();
                ele.put("cnc", CNCData.getList("cnc", Document.class));
                mongo.getDatabase(mongoDatabaseName).getCollection(CNCCollection)
                        .replaceOne(eq("_id", ele.getString("_id")), ele, new UpdateOptions().upsert(true));
//                        .findOneAndUpdate(eq("_id", ele.getString("_id")), new Document("$set", ele));
            }
        } else {
            MongoCursor<Document> unjoinedCursor = mongo.getDatabase(mongoDatabaseName).getCollection(AIMNotJoinedCollection)
                    .find(regex("_id", uniqueIDValue + AIMProcessValueOfCNC + "*")).iterator();
            if (unjoinedCursor.hasNext()) {
                /// TODO: collect AIM/CNC/SN data, do second join if necessary
                Document CNCData = getCNCData(uniqueIDValue);
                if (CNCData == null) {
                    log.error("Impossible !!! CNC cannot be empty, origin data - " + in.toJson());
                    return in;
                }

                while (unjoinedCursor.hasNext()) {
                    Document ele = unjoinedCursor.next();
                    ele.put("CNC", CNCData);
                    // TODO: validate the AIM data, if it has 'SN' & 'CNC', do second join
                    Document secondJoinData = AIMDataProcessor.doSecondJoin(ele);
                    if (secondJoinData != null) {
//                        log.info("-- Second Join --");
                        mongo.getDatabase(mongoDatabaseName).getCollection(AIMNotJoinedCollection)
                                .findOneAndDelete(eq("_id", ele.getString("_id")));
                        secondJoinData.put("_id", ele.getString("_id"));
                        mongo.getDatabase(mongoDatabaseName).getCollection(AIMCollection)
                                .replaceOne(eq("_id", ele.getString("_id")), secondJoinData, new UpdateOptions().upsert(true));
                    } else {
//                        log.info("cnc update unjoined data");
                        mongo.getDatabase(mongoDatabaseName).getCollection(AIMNotJoinedCollection)
                                .replaceOne(eq("_id", ele.getString("_id")), ele, new UpdateOptions().upsert(true));
//                                .findOneAndUpdate(eq("_id", ele.getString("_id")), new Document("$set", ele));
                    }
                }
            }
        }

        return null;
    }
    private Document getCNCData(String uniqueIDValue) {
        return getCNCData(uniqueIDValue, this.mongo);
    }
    public static Document getCNCData(String uniqueIDValue, MongoClient mongo) {
        MongoCursor<Document> batchCNCCursor =
                mongo.getDatabase(JComToo.T().getMongoDatabase()).getCollection(JComToo.T().getCNCCollection())
                        .find(regex("_id", uniqueIDValue + "*")).iterator();
        List<Document> listOfCNC = new ArrayList<>();
        if (!batchCNCCursor.hasNext()) {
            return null;
        }
        while (batchCNCCursor.hasNext()) {
            Document ele = batchCNCCursor.next();
            if (ele.containsKey(JComToo.T().getCNCProcessKey())
                    && ele.containsKey(JComToo.T().getCNCCellKey())
                    && ele.containsKey(JComToo.T().getCNCMachineKey())) {
                listOfCNC.add(
                        new Document(JComToo.T().getCNCProcessKey(), ele.getString(JComToo.T().getCNCProcessKey()))
                                .append(JComToo.T().getCNCCellKey(), ele.getString(JComToo.T().getCNCCellKey()))
                                .append(JComToo.T().getCNCMachineKey(), ele.getString(JComToo.T().getCNCMachineKey())));
            }
        }
        return new Document("cnc", listOfCNC);
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

    private String CNCCollection;
    private void store(String id, Document row) {
        row.put("_id", id);
        mongo.getDatabase(mongoDatabaseName).getCollection(CNCCollection)
                .replaceOne(eq("_id", id), row, new UpdateOptions().upsert(true));
    }

}
