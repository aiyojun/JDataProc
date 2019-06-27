package com.jpro;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

@Log4j2
class CoreProc {
    /**
     * Self member variables
     */
    boolean running = true;

    /**
     * Data source: Kafka settings
     */
    private KafkaConsumer<String, String> consumer;

    /**
     * Data filter
     */
    private AbstraFilter dataFilter;

    /**
     * Kernel member injection
     */
    CoreProc(AbstraFilter f) {
        dataFilter = f;
    }

    /**
     * Enter main loop
     */
    void start() {
        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.count() == 0) continue;
                log.info("Process data batch: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    
                }
            } catch (Exception e) {
                log.error("Kafka receive exception: " + e);
            }
        }
        log.info("Exiting main loop.");
    }

    /**
     * Close main loop, and recycle system resources
     */
    void close() {
        log.info("Invoke CoreProc::close()");
        running = false;
    }
}
