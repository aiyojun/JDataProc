package com.jpro;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Properties;

@Log4j2
class KafkaConsumer {
    private boolean isRunning = false;

    private Properties context;

    private DataAbstractProxy databaseProxy;

    private Thread localTask = new Thread(() -> {
        Properties props = new Properties();
        String kafkaUrl = context.getProperty("kafka.ip") + ":" + context.getProperty("kafka.port");
        props.setProperty("bootstrap.servers", kafkaUrl);
        props.setProperty("group.id", context.getProperty("kafka.group.id"));
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer
                = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        while (isRunning) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.count() == 0) continue;
                log.info("Process data batch: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    databaseProxy.insert(record.value());
                }
            } catch (Exception e) {
//                log.error("Occur err when process [ " + context.getProperty("kafka.topic") + " ] data: " + e);
            }
        }
    });

    /**
     * Ioc method to inject object
     */
    KafkaConsumer(DataAbstractProxy proxy) {
        databaseProxy = proxy;
    }

    void start() {
        // initialization
        isRunning = true;
        context = GlobalContext.getInstence().getProperties();

        localTask.start();
        log.info("KafkaConsumer task start");
    }

    void stop() {
        isRunning = false;
        log.info("KafkaConsumer task closed");
    }
}
