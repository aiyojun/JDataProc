package com.jpro.base;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

@Log4j2
public class KafkaProxy {

    private Properties context;

    private boolean isWorking = false;

    private KafkaConsumer<String, String> consumerOfAIM;

    private ArrayBlockingQueue<BaseBlock> sharedQueue;

    public KafkaProxy(Properties p, ArrayBlockingQueue<BaseBlock> q) {
        context = p;
        sharedQueue = q;
    }

    public void close() {
        isWorking = false;
    }

    public void prepare() {
        Properties conProps = makeBaseAttr();
        conProps.put("bootstrap.servers", context.getProperty("kafka.server.url"));
        conProps.put("group.id", context.getProperty("AIM.group.id"));
//        JComToo.log("\033[34;1m$$$$\033[0m KafkaProxy -- Kafka link ...");
        consumerOfAIM = new org.apache.kafka.clients.consumer.KafkaConsumer<>(conProps);
        consumerOfAIM.subscribe(Collections.singleton(context.getProperty("AIM.topic")));

    }

    public void start() {
        log.info("Kafka Proxy start.");
        log.info("kafka.server.url: " + context.getProperty("kafka.server.url"));
        log.info("aim-topic: " + context.getProperty("AIM.topic"));
        isWorking = true;
        while (isWorking) {
            ConsumerRecords<String, String> records = consumerOfAIM.poll(
                    Duration.ofMillis(Long.parseLong(context.getProperty("kafka.link.timeout"))));
            if (records.count() == 0) continue;
            for (ConsumerRecord<String, String> record : records) {
                try {
//                    JComToo.log("Kafka recv data: " + record.value());
                    sharedQueue.put(new BaseBlock(record.key(), record.value()));
                } catch (InterruptedException e) {
                    log.error("Push BaseBlock - " + e);
                }
            }
        }
    }

    public static Properties makeBaseAttr() {
        Properties conProps = new Properties();
        conProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return conProps;
    }

}
