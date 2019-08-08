package com.jpro;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

import static com.jpro.Unique.Utils.gtv;
import static com.jpro.Unique.Utils.gtv2int;
import static com.jpro.Unique.elfin;

@Log4j2
public class Provider extends Thread {

    private String _id;

    private ArrayBlockingQueue<Msg> sharedQueue;

    private KafkaConsumer<String, String> consumer;

    public Provider(ArrayBlockingQueue<Msg> q, String id) {
        sharedQueue = q;
        _id = id;
    }

    public Provider prepare(String topic, String groupID) {
        Properties pr = makeBaseAttr();
        pr.put("bootstrap.servers", gtv(elfin, "kafka.url"));
        pr.put("group.id", groupID);
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(pr);
        consumer.subscribe(Collections.singleton(topic));
        return this;
    }

    @Override
    public void run() {
        log.info("( {} ) Provider start", _id);
        while (Unique.working.get()) {
            ConsumerRecords<String, String> records = consumer.poll(
                    Duration.ofMillis(gtv2int(elfin, "kafka.timeout")));
            if (records.count() == 0) continue;
            for (ConsumerRecord<String, String> record : records) {
                try {
                    sharedQueue.put(new Msg(record.key(), record.value()));
                } catch (InterruptedException ignored) {

                }
            }
        }
        consumer.close();
        log.info("( {} ) Provider exiting ...", _id);
    }

    private static Properties makeBaseAttr() {
        Properties pr = new Properties();
        pr.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pr.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return pr;
    }

}
