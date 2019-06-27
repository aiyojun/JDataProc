package com.jpro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import lombok.extern.log4j.Log4j2;
import lombok.var;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

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
                    try {
                        Map<String, Object> piece = parseJsonString(record.value());
                        piece = dataFilter.filter(piece);

                    } catch (IOException ioe) {
                        log.error("IOException - " + ioe);
                    }
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

    /**
     * Util tool functions
     */
    static Map<String, Object> parseJsonString(String json) throws IOException {
        Map<String, Object> _r = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);
        for (var iter = root.fields(); iter.hasNext();) {
            var ele = iter.next();
            if (ele.getValue() instanceof TextNode) {
                _r.put(ele.getKey(), ele.getValue().asText());
            } else if (ele.getValue() instanceof IntNode) {
                _r.put(ele.getKey(), ele.getValue().asInt());
            } else if (ele.getValue() instanceof DoubleNode) {
                _r.put(ele.getKey(), ele.getValue().asDouble());
            } else if (ele.getValue() instanceof LongNode) {
                _r.put(ele.getKey(), ele.getValue().asLong());
            } else if (ele.getValue() instanceof BooleanNode) {
                _r.put(ele.getKey(), ele.getValue().asBoolean());
            } else {
                _r.put(ele.getKey(), ele.getValue().toString());
            }
        }
        return _r;
    }
}
