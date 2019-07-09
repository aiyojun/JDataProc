package com.jpro;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.time.Duration;
import java.util.*;

import static com.mongodb.client.model.Filters.*;

@Log4j2
class SubsTask {
    private Properties context;

    private MooPoo moo;

    SubsTask(Properties props, MooPoo poo) {
        context = props;
        moo = poo;
    }

    private boolean running;

    private KafkaConsumer<String, String> consumer;

    private void init() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", context.getProperty("notify.kafka.url"));
        props.setProperty("group.id", context.getProperty("notify.group.id"));
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(context.getProperty("notify.kafka.topic")));
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
                        Duration.ofMillis(Long.parseLong(context.getProperty("kafka.timeout"))));
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
                        System.out.println("-----");
                        doc.forEach((ky, vl) -> {
                            System.out.println(ky + ": " + vl);
                        });
                        process(record.key(), doc);
//                        doc.remove("_id");
//                        stationsAliasMapping.forEach((ky, val) -> {
//                            log.info("Update AIM : " + record.key() + "_" + val);
//                            ComToo.updateMongo(mongoClient, context.getProperty("storage.mongo.database"),
//                                    context.getProperty("storage.aim.collection"),
//                                    "_id", record.key() + "_" + val, doc);
//                        });
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

    private void process(String uni, Document orig) {
        orig.remove("_id");
        if (orig.containsKey(context.getProperty("sn.color.field"))
                && orig.containsKey(context.getProperty("sn.builds.field"))
                && orig.containsKey(context.getProperty("sn.special_build.field"))
                && orig.containsKey(context.getProperty("sn.wif_4g.field"))
        ) {
            log.info("from SN stream, do update (include MappingID field).");
            MongoClient mongoClient = moo.getMongoClient();
            try {
                List<Document> rows = new ArrayList<>();
                MongoCursor<Document> mongoCursor = mongoClient.getDatabase(context.getProperty("storage.mongo.database"))
                        .getCollection(context.getProperty("storage.aim.collection"))
                        .find(regex("_id", uni + "_*")).iterator();
                while (mongoCursor.hasNext()) {
                    rows.add(mongoCursor.next());
                }
                rows.forEach((lineOfAIM) -> {
                    orig.put(context.getProperty("mapping_id.key"), generateMappingIDForWangDong(lineOfAIM, orig));
                    mongoClient.getDatabase(context.getProperty("storage.mongo.database"))
                            .getCollection(context.getProperty("storage.aim.collection"))
                            .findOneAndUpdate(eq("_id", lineOfAIM.getString("_id")),
                                    new Document("$set", orig));
                });
            } catch (Exception e) {
                log.error(e);
            }
            moo.returnMongoClient(mongoClient);
        } else {
            MongoClient mongoClient = moo.getMongoClient();
            try {
                mongoClient.getDatabase(context.getProperty("storage.mongo.database"))
                        .getCollection(context.getProperty("storage.aim.collection"))
                        .updateMany(regex("_id", uni + "_*"), new Document("$set", orig));
            } catch (Exception e) {
                log.error(e);
            }
            moo.returnMongoClient(mongoClient);
        }
    }

    private String generateMappingIDForWangDong(Document fromAIM, Document fromSN) {
        return fromAIM.getString(context.getProperty("aim.site.key")) +
                fromAIM.getString(context.getProperty("aim.source.key")) +
                fromSN.getString(context.getProperty("sn.color.key")) +
                fromSN.getString(context.getProperty("sn.builds.key")) +
                fromSN.getString(context.getProperty("sn.special_build.key")) +
                fromSN.getString(context.getProperty("sn.wifi_4g.key"));
    }
}
