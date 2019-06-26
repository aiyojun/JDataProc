package com.jpro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import lombok.var;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

@Log4j2
public class Joinner {

    private boolean running = true;

    /**
     * some resources handler
     */
    private Properties context = GlobalContext.getInstence().getProperties();

    /**
     * Data source
     */
    private int kafkaPort;
    private String kafkaIp;
    private String streamMain0GroupId;
    private String streamLine1GroupId;
    private String streamLine2GroupId;
    private String streamMain0KafkaTopic;
    private String streamLine1KafkaTopic;
    private String streamLine2KafkaTopic;

    /**
     * Join logic
     */
    private String joinField;
    private int joinDemenstions;

    /**
     * Data storage
     */
    private int esPort;
    private String esIp;
    private String esIndex;
    private String esType;

    private JsonNode parse(String data) {
        JsonNode root = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            root = mapper.readTree(data);
        } catch (Exception e) {
            log.error("Parse data exception: " + e);
        }
        return root;
    }

    /**
     * filter mechanism
     * Default is not open;
     */
    private JsonNode doFilter(JsonNode root) {
        if (!context.getProperty("stream.filter.switch").equals("true")) return root;
        JsonNode _r = root;
        try {
            log.info("Joinner filter mechanism");
            List<String> filterFields = JComToo.parseCsv(context.getProperty("stream.main0.filter.fields"));
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> another = new HashMap<>();
            for (var it = root.fields(); it.hasNext();) {
                var ele = it.next();
                String key = ele.getKey();
                if (filterFields.contains(key)) continue;
                Object value = ele.getValue();
                another.put(key, value);
            }
            String s = mapper.writeValueAsString(another);
            _r = mapper.readTree(s);
            return _r;
        } catch (JsonProcessingException je) {
            log.error("When do filter, occur JsonProcessingException: " + je);
        } catch (IOException ie) {
            log.error("When do filter, occur IOException: " + ie);
        }
        return _r;
    }

    private JsonNode doJoin(JsonNode root) {
        JsonNode _r = root;

        return _r;
    }

    private String getLine1Record(String val) {

        return null;
    }

    private String getLine2Record(String val) {

        return null;
    }

    void main() {
        log.info("Enter Joinner process");
        Properties props = new Properties();
        String kafkaUrl = context.getProperty("kafka.ip") + ":" + context.getProperty("kafka.port");
        props.setProperty("bootstrap.servers", kafkaUrl);
        props.setProperty("group.id", context.getProperty("kafka.group.id"));
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.count() == 0) continue;
                log.info("Process data batch: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    var json = parse(record.value());
                    json = doFilter(json);
                    json = doJoin(json);
                }
            } catch (Exception e) {
                log.error("Kafka receive exception: " + e);
            }
        }
    }
}
