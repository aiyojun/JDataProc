package com.jpro;

import com.mongodb.MongoClient;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.time.Duration;
import java.util.*;

@Log4j2
public class Joinner {
    private boolean isWorking = false;

    /**
     * The necessary external injection
     */
    private Properties context;

    /**
     * Construct by self
     */
    private MongoClient mongoOfSN;
    private MongoClient mongoOfCNC;
    private MooPoo mongosOfAIM;

    private KafkaConsumer<String, String> consumerOfAIM;

    private Map<String, String> stationsAliasMapping;
    private Map<String, String> stationsOwnerMapping;

    Joinner(Properties pro, MooPoo poo, Map<String, String> alias, Map<String, String> owner) {
        context = pro;
        mongosOfAIM = poo;
        stationsAliasMapping = alias;
        stationsOwnerMapping = owner;
    }

    public void stop() {
        isWorking = false;
    }

    private void prepare() {
        /// Database storage settings
        mongoOfSN = new MongoClient(context.getProperty("storage.mongo.ip"),
                Integer.parseInt(context.getProperty("storage.mongo.port")));
        /// Data source
        Properties conProps = new Properties();
        conProps.put("bootstrap.servers", context.getProperty("source.kafka.url"));
        conProps.put("group.id", context.getProperty("source.group.id"));
        conProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerOfAIM = new org.apache.kafka.clients.consumer.KafkaConsumer<>(conProps);
        consumerOfAIM.subscribe(Collections.singleton(context.getProperty("source.kafka.topic")));
    }

    public void work() {
        prepare();
        isWorking = true;
        log.info("Wait Kafka ...");
        while (isWorking) {
            try {
                ConsumerRecords<String, String> records = consumerOfAIM.poll(
                        Duration.ofMillis(Long.parseLong(context.getProperty("kafka.timeout"))));
                if (records.count() == 0) continue;
                log.info("Process data batch: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // TODO: step 1 parse
                        Document row;
                        try {
                            row = Document.parse(record.value());
                        } catch (Exception e) {
                            log.error("Parse AIM data failed - " + record.value());
                            doStoreExceptionData(new Document("raw", record.value()));
                            continue;
                        }
                        log.info("Complete parse AIM data");

                        // TODO: step 2 check
                        try {
                            checkConditionOfAIM(row);
                        } catch (Exception e) {
                            log.error(e);
                            doStoreExceptionData(row);
                            continue;
                        }
                        log.info("Check AIM data");

                        // TODO: step 3 increase station
                        String stationType = row.getString(context.getProperty("station.key"));
                        if (!stationsAliasMapping.containsKey(stationType)) {
                            StringBuilder postfix = new StringBuilder();
                            postfix.append(context.getProperty("station.postfix"));
                            postfix.append(stationsAliasMapping.size());
                            stationsAliasMapping.put(stationType, postfix.toString());
                            stationsOwnerMapping.put(stationType, "-");
                        }

                        // TODO: step 4 query SN & CNC
                        Document inc1 = queryFromSN(row.getString(context.getProperty("unique.key")));

                        Document inc2;
                        if (stationsOwnerMapping.get(stationType).toUpperCase().equals("CNC")) {
                            log.info("==> AIM ++ CNC");
                            inc2 = queryFromCNC(row.getString(context.getProperty("unique.key")));
                        } else if (stationsOwnerMapping.get(stationType).toUpperCase().equals("SPM")) {
                            log.info("==> AIM ++ SPM");
                            inc2 = new Document();
                        } else {
                            log.info("==> AIM ++ -");
                            inc2 = new Document();
                        }

                        // TODO: step 5 merge
                        inc1.forEach((k, v) -> {
                            if (!k.equals(context.getProperty("unique.key"))) {
                                row.put(k, v);
                            }
                        });
                        inc2.forEach((k, v) -> {
                            if (!k.equals(context.getProperty("unique.key"))) {
                                row.put(k, v);
                            }
                        });
                        log.info("Merge");

                        // TODO: final store
                        finalStore("_id", row.getString(context.getProperty("unique.key"))
                                + "_" + stationsAliasMapping.get(stationType), row);
                        log.info("Store");
                    } catch (Exception e) {
                        log.error("----> " + e + " -> " + record.value());
                    }
                }
            } catch (Exception e) {
                log.error("--> " + e);
            }
        }
        log.info("Exiting Joinner work.");
    }

    private void finalStore(String key, String val, Document row) {
        row.remove("_id");
        MongoClient mongo = mongosOfAIM.getMongoClient();
        ComToo.upsertMongo(mongo, context.getProperty("storage.mongo.database"), context.getProperty("storage.aim.collection"), key, val, row);
        mongosOfAIM.returnMongoClient(mongo);
    }

    private Document queryFromSN(String unique) {
        Document res = ComToo.findOneMongo(mongoOfSN, context.getProperty("storage.mongo.database"), context.getProperty("storage.sn.collection"), "_id", unique);
        if (res == null) return new Document();
        return res;
    }

    private Document queryFromCNC(String unique) {
        List<Document> li = new ArrayList<>();
        Document res = new Document();
        log.info("sn number: " + unique);
        Document res1 = ComToo.findOneMongo(mongoOfCNC, context.getProperty("storage.mongo.database"), context.getProperty("storage.cnc.collection"), "_id", unique + "_CNC7");
        Document res2 = ComToo.findOneMongo(mongoOfCNC, context.getProperty("storage.mongo.database"), context.getProperty("storage.cnc.collection"), "_id", unique + "_CNC8");
        Document res3 = ComToo.findOneMongo(mongoOfCNC, context.getProperty("storage.mongo.database"), context.getProperty("storage.cnc.collection"), "_id", unique + "_CNC9");
        Document res4 = ComToo.findOneMongo(mongoOfCNC, context.getProperty("storage.mongo.database"), context.getProperty("storage.cnc.collection"), "_id", unique + "_CNC10");
        if (res1 != null) li.add(res1);
        if (res2 != null) li.add(res2);
        if (res3 != null) li.add(res3);
        if (res4 != null) li.add(res4);
        li.forEach((doc) -> {
            String processName = doc.getString("PROCESS_NAME");
            if (processName.contains("CNC7")) {
                res.append("CNC7_NAME", processName);
                res.append("CNC7_CELL", doc.getString("CELL"));
                res.append("CNC7_MC", doc.getString("MACHINE_NAME"));
            } else if (processName.contains("CNC8")) {
                res.append("CNC8_NAME", processName);
                res.append("CNC8_CELL", doc.getString("CELL"));
                res.append("CNC8_MC", doc.getString("MACHINE_NAME"));
            } else if (processName.contains("CNC9")) {
                res.append("CNC9_NAME", processName);
                res.append("CNC9_CELL", doc.getString("CELL"));
                res.append("CNC9_MC", doc.getString("MACHINE_NAME"));
            } else if (processName.contains("CNC10")) {
                res.append("CNC10_NAME", processName);
                res.append("CNC10_CELL", doc.getString("CELL"));
                res.append("CNC10_MC", doc.getString("MACHINE_NAME"));
            }
        });

        if (li.size() == 0) return new Document();
        return res;
    }

    private void checkConditionOfAIM(Document row) {
        if (!row.containsKey(context.getProperty("unique.key"))) {
            throw new RuntimeException("No " + context.getProperty("unique.key") + " in AIM data.");
        }
        if (!row.containsKey(context.getProperty("station.key"))) {
            throw new RuntimeException("No " + context.getProperty("station.key" + " in AIM data."));
        }
    }

    private void doStoreExceptionData(Document row) {
        row.remove("_id");
        MongoClient mongo = mongosOfAIM.getMongoClient();
        try {
            mongo.getDatabase(context.getProperty("storage.mongo.database"))
                    .getCollection(context.getProperty("storage.exc.collection"))
                    .insertOne(row);
        } catch (Exception e) {
            log.error(e);
        }
        mongosOfAIM.returnMongoClient(mongo);
        log.info("Porter::doStoreExceptionData complete");
    }
}
