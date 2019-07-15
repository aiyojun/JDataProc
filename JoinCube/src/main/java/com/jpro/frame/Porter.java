package com.jpro.frame;

import com.jpro.base.JComToo;
import com.jpro.base.KafkaProxy;
import com.jpro.proc.AbstractDataProcessor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.time.Duration;
import java.util.Properties;

@Log4j2
public class Porter {

    private boolean isWorking = false;

    /**
     * The necessary external injection
     */
    private Properties context;

    /**
     * Construct by self
     */
    private KafkaConsumer<String, String> consumer;

    private AbstractDataProcessor processor;

    Porter(Properties p, AbstractDataProcessor a, KafkaConsumer<String, String> c) {
        context = p;
        processor = a;
        consumer = c;
    }

    public void stop() {
        isWorking = false;
    }

    private void prepare() {

    }

    public void work() {
        JComToo.log("\033[34;1m$$$$\033[0m Porter Start To Work");
        isWorking = true;
        while (isWorking) {
            // TODO: poll data from Kafka
            ConsumerRecords<String, String> records = null;
            try {
                records = consumer.poll(Duration.ofMillis(Long.parseLong(context.getProperty("kafka.link.timeout"))));
            } catch (Exception e) {
                log.error("Kafka poll(Porter) - " + e);
                continue;
            }
            if (records == null || records.count() == 0) continue;

            for (ConsumerRecord<String, String> record : records) {
                // TODO: parse json
                Document origin = null;
                try {
                    origin = processor.parse(record.value());
                } catch (Exception e) {
                    log.error("Parse json(Porter) - " + e);
                    processor.trap(record.value());
                }
                if (origin == null) continue;

                // TODO: validate orignal data(fields)
                try {
                    processor.validate(origin);
                } catch (Exception e) {
                    log.error("Validate process(Porter) - " + e);
                    processor.trap(record.value());
                    continue;
                }

                // TODO: filter unnecessary data
                Document rowOfFilter = null;
                try {
                    rowOfFilter = processor.filter(origin);
                } catch (Exception e) {
                    log.error("Filter process(Porter) - " + e);
                    processor.trap(record.value());
                    continue;
                }
                if (rowOfFilter == null || rowOfFilter.isEmpty()) continue;

                // TODO: process data
                Document rowOfTransform = null;
                try {
                    rowOfTransform = processor.transform(rowOfFilter);
                } catch (Exception e) {
                    log.error("Transform process(Porter) - " + e);
                    processor.trap(record.value());
                    continue;
                }
                if (rowOfTransform == null) continue;

                // TODO: core process
                Document rowOfFinal = processor.core(rowOfTransform);

                // TODO: final process
                processor.post(rowOfFinal);
            }
        }
    }

}
