package com.jpro;

import com.mongodb.MongoClient;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Log4j2
class SubsTask {
    private Properties gProps;

    private MooPoo moo;

    private Map<String, String> stationsAliasMapping;
    private Map<String, String> stationsOwnerMapping;

    SubsTask(Properties props, MooPoo poo, Map<String, String> alias, Map<String, String> owner) {
        gProps = props;
        moo = poo;
        stationsAliasMapping = alias;
        stationsOwnerMapping = owner;
    }

    private boolean running;

    private KafkaConsumer<String, String> consumer;

    private void init() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", gProps.getProperty("notify.kafka.url"));
        props.setProperty("group.id", gProps.getProperty("notify.group.id"));
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(gProps.getProperty("notify.kafka.topic")));
    }

    void close() {
        running = false;
    }

    /**
     * accept key: sn value: standard CNC_data and standard SN_data
     */
    void start() {
        log.info("SubsTask start");
        init();
        running = true;
        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(
                        Duration.ofMillis(Long.parseLong(gProps.getProperty("kafka.timeout"))));
                if (records.count() == 0) continue;
                log.info("Batch: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    if (record.key() == null || record.key().isEmpty()) {
                        log.error("Notify Data lack of Kafka-Key [ {} ]", record.key());
                        continue;
                    }
                    MongoClient mongoClient = moo.getMongoClient();
                    try {
                        Document doc = Document.parse(record.value());
                        stationsAliasMapping.forEach((ky, val) -> {
                            log.info("Update AIM : " + record.key() + "_" + val);
                            ComToo.updateMongo(mongoClient, gProps.getProperty("storage.mongo.database"),
                                    gProps.getProperty("storage.aim.collection"),
                                    "_id", record.key() + "_" + val, doc);
                        });
                    } catch (Exception e) {
                        log.error(e);
                    }
                    moo.returnMongoClient(mongoClient);
                }
            } catch (Exception e) {
                log.error("--- Process subscribed data occur exception - " + e);
            }
        }
        log.info("Exiting Subscribe Task Main Loop.");
    }
}
