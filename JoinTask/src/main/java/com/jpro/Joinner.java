package com.jpro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import lombok.var;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

@Log4j2
class Joinner {
    /**
     * inner necessary member variables
     */
    private boolean running = true;

    /**
     * some resources handler
     */
    private Properties context = GlobalContext.getInstence().getProperties();

    /**
     * Data source
     */
    private RestHighLevelClient streamLine1Client;
    private RestHighLevelClient streamLine2Client;
    private KafkaConsumer<String, String> streamMain0Consumer;

    /**
     * Data storage
     */
    private RestHighLevelClient storeClient;

    private void init() {
        /// kafka connections
        Properties props = new Properties();
        String kafkaUrl = context.getProperty("kafka.ip") + ":" + context.getProperty("kafka.port");
        props.setProperty("bootstrap.servers", kafkaUrl);
        props.setProperty("group.id", context.getProperty("stream.main0.group.id"));
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        streamMain0Consumer = new KafkaConsumer<>(props);
        streamMain0Consumer.subscribe(Collections.singleton(context.getProperty("stream.main0.kafka.topic")));

        /// es connections
        streamLine1Client = new RestHighLevelClient(
            RestClient.builder(
                new HttpHost(context.getProperty("stream.line1.es.ip"),
                    new Integer(context.getProperty("stream.line1.es.port")), "http")));
        streamLine2Client = new RestHighLevelClient(
            RestClient.builder(
                new HttpHost(context.getProperty("stream.line2.es.ip"),
                    new Integer(context.getProperty("stream.line2.es.port")), "http")));
        storeClient = new RestHighLevelClient(
            RestClient.builder(
                new HttpHost(context.getProperty("output.es.ip"),
                    new Integer(context.getProperty("output.es.port")), "http")));
    }

    /**
     * recycle & release all resources
     */
    void close() {
        running = false;
        try {
            streamLine1Client.close();
            streamLine2Client.close();
        } catch (IOException ioe) {
            log.error("When closing es connections, occur exception: " + ioe);
        }
    }

    /**
     * parse json string
     */
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

    /**
     * cope with the data without the corresponding data of another one or two lines
     */
    private void doExceptionalData(String _id, String record) {
        IndexRequest req = new IndexRequest(
                context.getProperty("exception.es.index"),
                context.getProperty("exception.es.type"), _id);
        req.source(record, XContentType.JSON);
        try {
            IndexResponse resp = storeClient.index(req);
            if (resp.getResult() == DocWriteResponse.Result.NOOP) {
                throw new RuntimeException("insert failed, response - NOOP");
            } else if (resp.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                throw new RuntimeException("insert failed, response - NOT_FOUND");
            }
        } catch (IOException e) {
            log.error("When index exceptional data, occur io exception: " + e);
        }
    }

    /**
     * do join operation
     */
    private Map<String, Object> doJoin(JsonNode root, Map<String, Object> line1, Map<String, Object> line2) {
        Map<String, Object> _r = new HashMap<>();
        for (var iter = root.fields(); iter.hasNext();) {
            var ele = iter.next();
            if (ele.getValue().isTextual()) {
                _r.put(ele.getKey(), ele.getValue().textValue());
            } else if (ele.getValue().isNumber()) {
                _r.put(ele.getKey(), ele.getValue().numberValue());
            } else {
                _r.put(ele.getKey(), ele.getValue());
            }
        }
        line1.forEach((key, value) -> {
            if (!key.equals(context.getProperty("join.field"))) {
                _r.put(key, value);
            }
        });
        if (line2 != null) {
            line2.forEach((key, value) -> {
                if (!key.equals(context.getProperty("join.field"))) {
                    _r.put(key, value);
                }
            });
        }
        return _r;
    }

    /**
     * store the joined data
     */
    private void storeJoinedData(String _id, Map<String, Object> json) {
        IndexRequest req = new IndexRequest(
                context.getProperty("output.es.index"),
                context.getProperty("output.es.type"), _id);
        req.source(json);
        try {
            IndexResponse resp = storeClient.index(req);
            if (resp.getResult() == DocWriteResponse.Result.NOOP) {
                throw new RuntimeException("insert failed, response - NOOP");
            } else if (resp.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                throw new RuntimeException("insert failed, response - NOT_FOUND");
            }
        } catch (IOException e) {
            log.error("When index joined data, occur io exception: " + e);
        }
    }

    /**
     * obtain corresponding data from es database
     */
    private Map<String, Object> getLine1Record(String _id) {
        GetRequest req = new GetRequest(
                context.getProperty("stream.line1.es.index"), context.getProperty("stream.line1.es.type"), _id);
        try {
            GetResponse resp = streamLine1Client.get(req);
            return resp.getSource();
        } catch (IOException ioe) {
            log.error("When obtain stream line1 data, occur exception: " + ioe);
        }
        return null;
    }

    /**
     * another table
     * obtain corresponding data from es database
     */
    private Map<String, Object> getLine2Record(String _id) {
        GetRequest req = new GetRequest(
                context.getProperty("stream.line2.es.index"), context.getProperty("stream.line2.es.type"), _id);
        try {
            GetResponse resp = streamLine1Client.get(req);
            return resp.getSource();
        } catch (IOException ioe) {
            log.error("When obtain stream line1 data, occur exception: " + ioe);
        }
        return null;
    }

    /**
     * main loop of join task
     */
    void main() {
        log.info("Enter Joinner process");
        init();
        while (running) {
            try {
                ConsumerRecords<String, String> records = streamMain0Consumer.poll(Duration.ofMillis(1000));
                if (records.count() == 0) continue;
                log.info("Process data batch: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        var json = parse(record.value());
                        json = doFilter(json);
                        String _id = json.path(context.getProperty("join.field")).toString();
                        if (context.getProperty("join.dimensions").equals("2")) {
                            Map<String, Object> line1Record = getLine1Record(_id);
                            Map<String, Object> line2Record = getLine2Record(_id);
                            if (line1Record != null && line2Record != null) {
                                var result = doJoin(json, line1Record, line2Record);
                                storeJoinedData(_id, result);
                            } else {
                                if (line1Record == null) {
                                    log.warn("Cannot find data which "
                                            + context.getProperty("join.field") + "=" + _id
                                            + " from " + context.getProperty("stream.line1.es.index") + "/"
                                            + context.getProperty("stream.line1.es.type"));
                                } else {
                                    log.warn("Cannot find data which "
                                            + context.getProperty("join.field") + "=" + _id
                                            + " from " + context.getProperty("stream.line2.es.index") + "/"
                                            + context.getProperty("stream.line2.es.type"));
                                }
                                doExceptionalData(_id, record.value());
                            }
                        } else {
                            Map<String, Object> line1Record = getLine1Record(_id);
                            if (line1Record != null) {
                                var result = doJoin(json, line1Record, null);
                                storeJoinedData(_id, result);
                            } else {
                                log.warn("Cannot find data which "
                                        + context.getProperty("join.field") + "=" + _id
                                        + " from " + context.getProperty("stream.line1.es.index") + "/"
                                        + context.getProperty("stream.line1.es.type"));
                                doExceptionalData(_id, record.value());
                            }
                        }

                    } catch (Exception e) {
                        log.error(e);
                    }
                }
            } catch (Exception e) {
                log.error("Kafka receive exception: " + e);
            }
        }
        log.info("Exit join task main loop.");
    }
}
