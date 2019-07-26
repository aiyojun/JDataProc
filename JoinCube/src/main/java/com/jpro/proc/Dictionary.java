package com.jpro.proc;

import com.jpro.base.GlobalContext;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Description get dictionary data
 * @Date 2019/7/11 12:20
 * @Created by King
 */
@Log4j2
public class Dictionary {

//    private static Dictionary self;
//    public static Dictionary getInstance() {
//        if (self == null) {
//            self = new Dictionary(GlobalContext.context);
//        }
//        return self;
//    }

    private static Properties context;
    private Map<String, Map<String, String>> colorDictMap;
    private static ReentrantLock lock = new ReentrantLock();
    private KafkaConsumer<String, String> consumer;
    private long kafkaTimeout;

    public Dictionary(Properties pro) {
        context = pro;
//        colorDictMap = getData(pro);

        Properties conProps = new Properties();
        conProps.put("bootstrap.servers", pro.getProperty("dictionary.kafka.url"));
        conProps.put("group.id", context.getProperty("dictionary.consumer.group.id"));
        conProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(conProps);
        consumer.subscribe(Collections.singleton(pro.getProperty("dictionary.kafka.topic")));
        kafkaTimeout = Long.parseLong(pro.getProperty("kafka.link.timeout"));
        getMsgThread.start();
    }

    private Map<String, Map<String, String>> getData(Properties context) {
        Map<String, Map<String, String>> map = new HashMap<>();
        try {
//            try {
            MongoClient mongoClient = new MongoClient(context.getProperty("dictionary.mongo.ip"),
                    Integer.parseInt(context.getProperty("dictionary.mongo.port")));
//            } catch (Exception e) {
//                log.error("Mongo link failed - " + e);
//            }

            lock.lock();
            MongoCursor<Document> mongoCursor = mongoClient.getDatabase(context.getProperty("dictionary.mongo.database"))
                    .getCollection(context.getProperty("dictionary.mongo.collection"))
                    .find().iterator();

            while (mongoCursor.hasNext()) {
                Document next = mongoCursor.next();
                if (!map.containsKey(next.getString(context.getProperty("SN.part_id.key")))) {
                    map.put(next.getString(context.getProperty("SN.part_id.key")), new HashMap<>());
                }
                Map<String, String> colorDict = map.get(next.getString(context.getProperty("SN.part_id.key")));
                colorDict.put(context.getProperty("SN.product_code.key"), next.getString(context.getProperty("SN.material_type.key")));
                colorDict.put(context.getProperty("SN.wifi_4g.key"), next.getString(context.getProperty("SN.jancode.key")));
                colorDict.put(context.getProperty("SN.color.key"), next.getString(context.getProperty("SN.upccode.key")));
            }
            mongoClient.close();
        } finally {
            lock.unlock();
        }
        return map;
    }

    Thread getMsgThread = new Thread(() -> {
//        while (!Thread.currentThread().isInterrupted()) {
        while (GlobalContext.isWorking.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(kafkaTimeout));
            records.forEach(record -> {
//                if ("sn".equalsIgnoreCase(record.value())) {
                colorDictMap = getData(context);
//                }
            });
        }
        log.info("Dictionary exit loop!");
        consumer.close();
    });

    public Map<String, String> get(String key) {
        if (colorDictMap == null) {
            colorDictMap = getData(context);
        }
        try {
            lock.lock();
            if (colorDictMap.containsKey(key))
                return colorDictMap.get(key);
        } finally {
            lock.unlock();
        }
        return null;
    }

}
