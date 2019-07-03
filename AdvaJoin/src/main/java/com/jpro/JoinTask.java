package com.jpro;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.MongoClient;
import lombok.extern.log4j.Log4j2;
import lombok.var;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

@Log4j2
class JoinTask {
    private Properties gProps;

    private StoreAcces storeAcces;

    JoinTask(Properties props, StoreAcces store, Map<String, String> alias, Map<String, String> owner) {
        gProps = props;
        storeAcces = store;
        stationsAliasMapping = alias;
        stationsOwnerMapping = owner;
    }

    private boolean running;

    void close() {
        running = false;
    }

    private KafkaConsumer<String, String> consumer;

    private Map<String, String> stationsAliasMapping;
    private Map<String, String> stationsOwnerMapping;

    private void init() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", gProps.getProperty("kafka.aim.ip") + ":" + gProps.getProperty("kafka.aim.port"));
        props.setProperty("group.id", gProps.getProperty("kafka.aim.group.id"));
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(gProps.getProperty("kafka.aim.topic")));
    }

    void start() {
        log.info("JoinTask start");
        init();
        running = true;
        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(
                        Duration.ofMillis(Long.parseLong(gProps.getProperty("kafka.timeout"))));
                if (records.count() == 0) continue;
                log.info("Process data batch: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    StringBuilder _id = new StringBuilder();
                    // AIM Data
                    JsonNode AIM_J;
                    try {
                        AIM_J = parseAIMData(record.value());
                    } catch (Exception e) {
                        log.error(e);
                        continue;
                    }
                    _id.append(AIM_J.path(gProps.getProperty("unique.key")).textValue());
                    String sn = _id.toString();

                    boolean isCNCBranch = true;
                    String stationType = AIM_J.path(gProps.getProperty("station.field")).textValue();
                    if (!stationsAliasMapping.containsKey(stationType)) {
                        log.error("No such AIM station " + AIM_J.path(gProps.getProperty("station.field")).textValue());
                        processExceptionData(AIM_J);
                        continue;
                    }
                    if (stationsOwnerMapping.get(stationType).toUpperCase().equals("SPM")) {
                        isCNCBranch = false;
                    }
                    _id.append("_").append(stationsAliasMapping.get(stationType));

                    // CNC / SPM Data
                    Document CNC_OR_SPM_D;
                    if (isCNCBranch) {
                        log.info("Join [ AIM ] ++++ [ CNC ]");
                        CNC_OR_SPM_D = queryCNCData(sn);
                    } else {
                        log.info("Join [ AIM ] ++++ [ SPM ]");
                        CNC_OR_SPM_D = querySPMData(sn);
                    }

                    //  SN Data
                    Document SNN_D = querySNData(sn);

                    Document ALL_D = new Document("_id", _id.toString());
                    for (var iter = AIM_J.fields(); iter.hasNext();) {
                        var ele = iter.next();
                        if (ele.getValue().isTextual()) {
                            ALL_D.append(ele.getKey(), ele.getValue().textValue());
                        } else if (ele.getValue().isNumber()) {
                            ALL_D.append(ele.getKey(), ele.getValue().numberValue());
                        }
                    }
                    CNC_OR_SPM_D.forEach(ALL_D::append);
                    SNN_D.forEach((k, v) -> {
                        if (!k.equals(gProps.getProperty("unique.key"))) {
                            ALL_D.append(k, v);
                        }
                    });
                    // TODO: storage
                    store("_id", _id.toString(), ALL_D);
                }
            } catch (Exception e) {
                log.error("Kafka receive exception: " + e);
            }
        }
        log.info("Exiting Join Task Main Loop.");
    }

    private void store(String key, String val, Document row) {
        MooPoo mooPoo = storeAcces.getAIM_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        ComToo.upsertMongo(mongoClient, gProps.getProperty("mongo.aim.database"), gProps.getProperty("mongo.aim.collection"), key, val, row);
        mooPoo.returnMongoClient(mongoClient);
    }

    private Document querySNData(String uniKeyVal) {
        MooPoo mooPoo = storeAcces.getSNN_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        Document res = ComToo.findOneMongo(mongoClient, gProps.getProperty("mongo.sn.database"), gProps.getProperty("mongo.sn.collection"), "_id", uniKeyVal);
        if (res == null) return new Document();
        mooPoo.returnMongoClient(mongoClient);
        return res;
    }

    private Document querySPMData(String uniKeyVal) {
        MooPoo mooPoo = storeAcces.getSPM_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        Document res = ComToo.findOneMongo(mongoClient, gProps.getProperty("mongo.spm.database"), gProps.getProperty("mongo.spm.collection"), "_id", uniKeyVal);
        if (res == null) return new Document();
        mooPoo.returnMongoClient(mongoClient);
        return res;
    }

    private Document queryCNCData(String uniKeyVal) {
        MooPoo mooPoo = storeAcces.getCNC_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        List<Document> li = new ArrayList<>();
        Document res = new Document();
        log.info("sn number: " + uniKeyVal);
        Document res1 = ComToo.findOneMongo(mongoClient, gProps.getProperty("mongo.cnc.database"), gProps.getProperty("mongo.cnc.collection"), "_id", uniKeyVal + "_CNC7");
        Document res2 = ComToo.findOneMongo(mongoClient, gProps.getProperty("mongo.cnc.database"), gProps.getProperty("mongo.cnc.collection"), "_id", uniKeyVal + "_CNC8");
        Document res3 = ComToo.findOneMongo(mongoClient, gProps.getProperty("mongo.cnc.database"), gProps.getProperty("mongo.cnc.collection"), "_id", uniKeyVal + "_CNC9");
        Document res4 = ComToo.findOneMongo(mongoClient, gProps.getProperty("mongo.cnc.database"), gProps.getProperty("mongo.cnc.collection"), "_id", uniKeyVal + "_CNC10");
        mooPoo.returnMongoClient(mongoClient);
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

    private JsonNode parseAIMData(String json) {
        JsonNode _r;
        try {
            _r = ComToo.parseJsonString(json);
        } catch (IOException e) {
            processExceptionData(json);
            throw new RuntimeException("Parse json error, AIM data [ " + json + " ]");
        }
        if (!_r.has(gProps.getProperty("unique.key")) || !_r.has(gProps.getProperty("station.field"))) {
            processExceptionData(_r);
            throw new RuntimeException("Invalid AIM data [ " + json + " ]");
        }
        return _r;
    }

    private void processExceptionData(String data) {
        MooPoo mooPoo = storeAcces.getEXC_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        Document row = new Document("raw", data);
        ComToo.insert(mongoClient, gProps.getProperty("mongo.exc.database"), gProps.getProperty("mongo.exc.collection"), row);
        mooPoo.returnMongoClient(mongoClient);
    }

    private void processExceptionData(JsonNode data) {
        MooPoo mooPoo = storeAcces.getEXC_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        Document row = new Document();
        for (var iter = data.fields(); iter.hasNext();) {
            var ele = iter.next();
            if (ele.getValue().isTextual()) {
                row.append(ele.getKey(), ele.getValue().textValue());
            } else if (ele.getValue().isNumber()) {
                row.append(ele.getKey(), ele.getValue().numberValue());
            } else {
                row.append(ele.getKey(), ele.getValue());
            }
        }
        ComToo.insert(mongoClient, gProps.getProperty("mongo.exc.database"), gProps.getProperty("mongo.exc.collection"), row);
        mooPoo.returnMongoClient(mongoClient);
    }
/*
    private void processExceptionData(Map<String, Object> data) {
        MooPoo mooPoo = storeAcces.getEXC_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        Document row = new Document();
        data.forEach(row::append);
        ComToo.insert(mongoClient, "mydb", "exception_data", row);
        mooPoo.returnMongoClient(mongoClient);
    }*/
}
