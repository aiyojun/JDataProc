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

    private StoreAcces storeAcces;

    SubsTask(Properties props, StoreAcces store) {
        gProps = props;
        storeAcces = store;
    }

    private boolean running;

    private KafkaConsumer<String, String> consumer;

    private void init() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", gProps.getProperty("kafka.update.ip") + ":" + gProps.getProperty("kafka.update.port"));
        props.setProperty("group.id", gProps.getProperty("kafka.update.group.id"));
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(gProps.getProperty("kafka.update.topic")));
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
        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.parseLong(gProps.getProperty("kafka.timeout"))));
                if (records.count() == 0) continue;
                log.info("Process data batch: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    MongoClient mongoClient = storeAcces.getAIM_MOO().getMongoClient();
                    try {
                        Map<String, Object> fromKafka = ComToo.parseJson(record.value());
                        Document doc = new Document();
                        fromKafka.forEach(doc::append);
                        ComToo.updateMongo(mongoClient, gProps.getProperty("mongo.aim.database"),
                                gProps.getProperty("mongo.aim.collection"), "_id", record.key(), doc);
                    } catch (Exception e) {
                        log.error(e);
                    }
                    storeAcces.getAIM_MOO().returnMongoClient(mongoClient);
                }
            } catch (Exception e) {
                log.error("Kafka receive exception: " + e);
            }
        }
    }
}
