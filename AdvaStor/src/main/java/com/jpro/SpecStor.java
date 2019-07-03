package com.jpro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.log4j.Log4j2;
import lombok.var;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.mongodb.client.model.Filters.eq;

@Log4j2
public class SpecStor {
    private MooPoo ExcMoo;
    private MooPoo CncMoo;
    private MooPoo AimMoo;

    private boolean running = false;

    public void close() {
        running = false;
    }

    private KafkaConsumer<String, String> consumer;

    private Producer<String, String> producer;


    private Properties props;

    SpecStor(Properties p, MooPoo exc, MooPoo aim, MooPoo cnc) {
        props = p;
        ExcMoo = exc;
        AimMoo = aim;
        CncMoo = cnc;
    }

    private void init() {
        Properties props1 = new Properties();
        if (props.getProperty("stream.type").equals("sn")) {
            props1.setProperty("bootstrap.servers", props.getProperty("kafka.sn.ip") + ":" + props.getProperty("kafka.sn.port"));
            props1.setProperty("group.id", props.getProperty("kafka.sn.group.id"));
        } else {
            props1.setProperty("bootstrap.servers", props.getProperty("kafka.cnc.ip") + ":" + props.getProperty("kafka.cnc.port"));
            props1.setProperty("group.id", props.getProperty("kafka.cnc.group.id"));
        }
        props1.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props1.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props1);
        if (props.getProperty("stream.type").equals("sn")) {
            consumer.subscribe(Collections.singleton(props.getProperty("kafka.sn.topic")));
        } else {
            consumer.subscribe(Collections.singleton(props.getProperty("kafka.cnc.topic")));
        }

        Properties props2 = new Properties();
        props2.put("bootstrap.servers", props.getProperty("publish.data.kafka.ip") + ":" + props.getProperty("publish.data.kafka.port"));
        props2.put("acks", "all");
        props2.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props2.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props2);
    }

    public void startForSN() {
        running = true;
        init();
        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.parseLong(props.getProperty("kafka.timeout"))));
                if (records.count() == 0) continue;
                log.info("Process data batch: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    JsonNode root;
                    try {
                        root = ComToo.parseJsonString(record.value());
                    } catch (IOException e) {
                        processExceptionData(record.value());
                        log.error("Parse json failed, " + e);
                        continue;
                    }
                    if (!root.has(props.getProperty("unique.key"))) {
                        processExceptionData(root);
                        log.error("Invalid SN data [ " + record.value() + " ].");
                        continue;
                    }
                    // query
                    StringBuilder _id = new StringBuilder();
                    _id.append(root.path(props.getProperty("unique.key")).textValue());
                    MongoClient mongoClient = AimMoo.getMongoClient();
                    boolean hasKey = ComToo.askMongoHasKey(mongoClient, props.getProperty("mongo.aim.database"), props.getProperty("mongo.aim.collection"), "_id", _id.toString());
                    AimMoo.returnMongoClient(mongoClient);
                    if (hasKey) { // update AIM data
                        publishUpdateData(_id.toString(), record.value());
                    }

                    // insert / update
                    Document row = new Document();
                    for (var iter = root.fields(); iter.hasNext();) {
                        var ele = iter.next();
                        if (ele.getValue().isTextual()) {
                            row.put(ele.getKey(), ele.getValue().textValue());
                        } else if (ele.getValue().isNumber()) {
                            row.put(ele.getKey(), ele.getValue().numberValue());
                        } else {
                            row.put(ele.getKey(), ele.getValue());
                        }
                    }
                    MongoClient mongoClient1 = CncMoo.getMongoClient();
                    mongoClient1.getDatabase(props.getProperty("mongo.sn.database")).getCollection(props.getProperty("mongo.sn.collection"))
                            .replaceOne(eq("_id", _id.toString()), row, new UpdateOptions().upsert(true));
                    CncMoo.returnMongoClient(mongoClient1);
                }
            } catch (Exception e) {
                log.error("Kafka receive exception: " + e);
            }
        }
    }

    public void start() {
        running = true;
        init();
        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.parseLong(props.getProperty("kafka.timeout"))));
                if (records.count() == 0) continue;
                log.info("Process data batch: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    JsonNode root;
                    try {
                        root = ComToo.parseJsonString(record.value());
                    } catch (IOException e) {
                        processExceptionData(record.value());
                        log.error("Parse json failed, " + e);
                        continue;
                    }
                    if (!root.has(props.getProperty("unique.key")) || !root.has(props.getProperty("cnc.distinct.key")) || !root.has(props.getProperty("cnc.filter.key"))) {
                        processExceptionData(root);
                        log.error("Invalid CNC data [ " + record.value() + " ].");
                        continue;
                    }
                    // filter
                    if (!root.path(props.getProperty("cnc.filter.key")).textValue().contains("CNC10")) {
                        log.info("Filtered " + root.path(props.getProperty("cnc.filter.key")).textValue());
                        continue;
                    }
                    // query
                    StringBuilder _id = new StringBuilder();
                    _id.append(root.path(props.getProperty("unique.key")).textValue());
                    MongoClient mongoClient = AimMoo.getMongoClient();
                    boolean hasKey = ComToo.askMongoHasKey(mongoClient, props.getProperty("mongo.aim.database"),
                            props.getProperty("mongo.aim.collection"), "_id", _id.toString() + "_*");
                    AimMoo.returnMongoClient(mongoClient);
                    if (hasKey) { // update AIM data
                        log.info("Find old record in AIM table, now update!");
                        try {
                            publishUpdateData(_id.toString(), reshapeCNCData(root));
                        } catch (JsonProcessingException e) {
                            log.error(e);
                        }
                    } else {
                        log.info("No old record in AIM table");
                    }

                    // insert / update
                    if (root.path(props.getProperty("cnc.distinct.key")).textValue().contains("CNC7")) {
                        _id.append("_CNC7");
                    } else if (root.path(props.getProperty("cnc.distinct.key")).textValue().contains("CNC8")) {
                        _id.append("_CNC8");
                    } else if (root.path(props.getProperty("cnc.distinct.key")).textValue().contains("CNC9")) {
                        _id.append("_CNC9");
                    } else if (root.path(props.getProperty("cnc.distinct.key")).textValue().contains("CNC10")) {
                        _id.append("_CNC10");
                    } else {
                        log.error("Invalid CNC data, cannot find specify CNC number in "
                                + props.getProperty("cnc.distinct.key") + " [ " + record.value() + " ]");
                        continue;
                    }
                    Document row = new Document();
                    for (var iter = root.fields(); iter.hasNext();) {
                        var ele = iter.next();
                        if (ele.getValue().isTextual()) {
                            row.put(ele.getKey(), ele.getValue().textValue());
                        } else if (ele.getValue().isNumber()) {
                            row.put(ele.getKey(), ele.getValue().numberValue());
                        } else {
                            row.put(ele.getKey(), ele.getValue());
                        }
                    }
                    MongoClient mongoClient1 = CncMoo.getMongoClient();
                    mongoClient1.getDatabase(props.getProperty("mongo.cnc.database")).getCollection(props.getProperty("mongo.cnc.collection"))
                            .replaceOne(eq("_id", _id.toString()), row, new UpdateOptions().upsert(true));
                    CncMoo.returnMongoClient(mongoClient1);
                }
            } catch (Exception e) {
                log.error("Kafka receive exception: " + e);
            }
        }
        log.info("Exiting Main Loop");
    }

    private String reshapeCNCData(JsonNode root) throws JsonProcessingException {
        Map<String, Object> row = new HashMap<>();
        if (root.path(props.getProperty("cnc.distinct.key")).textValue().contains("CNC7")) {
            row.put("CNC7_NAME", root.path(props.getProperty("cnc.distinct.key")).textValue());
            row.put("CNC7_CELL", root.path("CELL").textValue());
            row.put("CNC7_MC", root.path("MACHINE_NAME").textValue());
        } else if (root.path(props.getProperty("cnc.distinct.key")).textValue().contains("CNC8")) {
            row.put("CNC8_NAME", root.path(props.getProperty("cnc.distinct.key")).textValue());
            row.put("CNC8_CELL", root.path("CELL").textValue());
            row.put("CNC8_MC", root.path("MACHINE_NAME").textValue());
        } else if (root.path(props.getProperty("cnc.distinct.key")).textValue().contains("CNC9")) {
            row.put("CNC9_NAME", root.path(props.getProperty("cnc.distinct.key")).textValue());
            row.put("CNC9_CELL", root.path("CELL").textValue());
            row.put("CNC9_MC", root.path("MACHINE_NAME").textValue());
        } else if (root.path(props.getProperty("cnc.distinct.key")).textValue().contains("CNC10")) {
            row.put("CNC10_NAME", root.path(props.getProperty("cnc.distinct.key")).textValue());
            row.put("CNC10_CELL", root.path("CELL").textValue());
            row.put("CNC10_MC", root.path("MACHINE_NAME").textValue());
        }
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(row);
    }

    private void publishUpdateData(String _id, String json) {
        log.info("Publish key [ " + _id + " ], value [ " + json + " ].");
        producer.send(new ProducerRecord<>(props.getProperty("publish.data.kafka.topic"), _id, json));
    }

    private void processExceptionData(String data) {
        MongoClient mongoClient = ExcMoo.getMongoClient();
        Document row = new Document("raw", data);
        ComToo.insert(mongoClient, props.getProperty("mongo.exc.database"), props.getProperty("mongo.exc.collection"), row);
        ExcMoo.returnMongoClient(mongoClient);
    }

    private void processExceptionData(JsonNode data) {
        MongoClient mongoClient = ExcMoo.getMongoClient();
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
        ComToo.insert(mongoClient, props.getProperty("mongo.exc.database"), props.getProperty("mongo.exc.collection"), row);
        ExcMoo.returnMongoClient(mongoClient);
    }
}
