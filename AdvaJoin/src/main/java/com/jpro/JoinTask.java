package com.jpro;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.MongoClient;
import lombok.extern.log4j.Log4j2;
import lombok.var;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import javax.print.attribute.standard.DocumentName;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

@Log4j2
class JoinTask {
    private Properties gProps;

    private StoreAcces storeAcces;

    JoinTask(Properties props, StoreAcces store) {
        gProps = props;
        storeAcces = store;
    }

    private boolean running;

    void close() {
        running = false;
    }

    private KafkaConsumer<String, String> consumer;

    void init() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", gProps.getProperty("monitor.kafka.ip") + ":" + gProps.getProperty("monitor.kafka.port"));
        props.setProperty("group.id", gProps.getProperty("monitor.group.id"));
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(gProps.getProperty("monitor.source.topic")));
    }

    void start() {
        log.info("JoinTask start");
        init();
        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.parseLong(gProps.getProperty("join.kafka.timeout"))));
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
                    _id.append(AIM_J.path("SERIAL_NUMBER").textValue());

                    if (AIM_J.path("STATION").textValue().equals("station1")) {
                        _id.append("_ST1");
                    } else if (AIM_J.path("STATION").textValue().equals("station2")) {
                        _id.append("_ST2");
                    } else {
                        log.error("No such AIM station " + AIM_J.path("STATION").textValue());
                        processExceptionData(AIM_J);
                        continue;
                    }

                    // CNC Data
                    Document CNC_D = queryCNCData(_id.toString());

                    //  SN Data
                    Document SNN_D = querySNData(_id.toString());

                    Document ALL_D = new Document("_id", _id);
                    for (var iter = AIM_J.fields(); iter.hasNext();) {
                        var ele = iter.next();
                        if (ele.getValue().isTextual()) {
                            ALL_D.append(ele.getKey(), ele.getValue().textValue());
                        } else if (ele.getValue().isNumber()) {
                            ALL_D.append(ele.getKey(), ele.getValue().numberValue());
                        }
                    }
                    CNC_D.forEach(ALL_D::append);
                    SNN_D.forEach((k, v) -> {
                        if (!k.equals("SERIAL_NUMBER")) {
                            ALL_D.append(k, v);
                        }
                    });

                    // TODO: storage
                    store(ALL_D);
                }
            } catch (Exception e) {
                log.error("Kafka receive exception: " + e);
            }
        }
    }

    private void store(Document row) {
        MooPoo mooPoo = storeAcces.getSTO_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        ComToo.insert(mongoClient, "mydb", "aim_cnc_sn_data", row);
        mooPoo.returnMongoClient(mongoClient);
    }

    private Document querySNData(String uniKeyVal) {
        MooPoo mooPoo = storeAcces.getSNN_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        Document res = ComToo.findOneMongo(mongoClient, "mydb", "sn_data", "_id", uniKeyVal);
        if (res == null) return new Document();
        mooPoo.returnMongoClient(mongoClient);
        return res;
    }

    private Document queryCNCData(String uniKeyVal) {
        MooPoo mooPoo = storeAcces.getCNC_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        List<Document> li = new ArrayList<>();
        Document res = new Document();
        Document res1 = ComToo.findOneMongo(mongoClient, "mydb", "cnc_data", "_id", uniKeyVal + "_CNC7");
        Document res2 = ComToo.findOneMongo(mongoClient, "mydb", "cnc_data", "_id", uniKeyVal + "_CNC8");
        Document res3 = ComToo.findOneMongo(mongoClient, "mydb", "cnc_data", "_id", uniKeyVal + "_CNC9");
        Document res4 = ComToo.findOneMongo(mongoClient, "mydb", "cnc_data", "_id", uniKeyVal + "_CNC10");
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
        if (!_r.has("SERIAL_NUMBER") || !_r.has("STATION")) {
            processExceptionData(_r);
            throw new RuntimeException("Invalid AIM data [ " + json + " ]");
        }
        return _r;
    }

    private void processExceptionData(String data) {
        MooPoo mooPoo = storeAcces.getEXC_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        Document row = new Document("raw", data);
        ComToo.insert(mongoClient, "mydb", "exception_data", row);
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
        ComToo.insert(mongoClient, "mydb", "exception_data", row);
        mooPoo.returnMongoClient(mongoClient);
    }

    private void processExceptionData(Map<String, Object> data) {
        MooPoo mooPoo = storeAcces.getEXC_MOO();
        MongoClient mongoClient = mooPoo.getMongoClient();
        Document row = new Document();
        data.forEach(row::append);
        ComToo.insert(mongoClient, "mydb", "exception_data", row);
        mooPoo.returnMongoClient(mongoClient);
    }
}
