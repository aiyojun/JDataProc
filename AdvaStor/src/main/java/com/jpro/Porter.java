package com.jpro;

import com.mongodb.MongoClient;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;

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
    private MongoClient mongo;

    private KafkaConsumer<String, String> consumer;

    private Producer<String, String> producer;

    /**
     * logic control
     */
    private boolean needFilter = false;

    private boolean needNotify = true;

    private boolean needReshape = false;

    private Filter selfFilter;

    Porter(Properties pro) {
        context = pro;
    }

    private void prepare() {
        if (context.getProperty("filter.switch").toLowerCase().equals( "true")) {
            needFilter = true;
            selfFilter = new CNCDataFilter();
        }
        if (context.getProperty("notify.switch").toLowerCase().equals("false")) needNotify = false;
        if (context.getProperty("reshape.switch").toLowerCase().equals("true")) needReshape = true;
        /// Database storage settings
        mongo = new MongoClient(context.getProperty("storage.mongo.ip"),
                Integer.parseInt(context.getProperty("storage.mongo.port")));
        /// Data source
        Properties conProps = new Properties();
        conProps.put("bootstrap.servers", context.getProperty("source.kafka.url"));
        conProps.put("group.id", context.getProperty("consumer.group.id"));
        conProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(conProps);
        consumer.subscribe(Collections.singleton(context.getProperty("source.kafka.topic")));
        /// Notify up-to-date data
        Properties proProps = new Properties();
        proProps.put("bootstrap.servers", context.getProperty("notify.kafka.url"));
        proProps.put("acks", "all");
        proProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        proProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(proProps);
    }

    public void stop() {
        isWorking = false;
    }

    public void work() {
        prepare();
        isWorking = true;
        while (isWorking) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.parseLong(context.getProperty("kafka.timeout"))));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Document row = Document.parse(record.value());
                        if (needFilter) {
                            row = selfFilter.doFilter(row);
                        }

                        if (needReshape) {

                        }

                        if (row != null && !row.isEmpty()) {
                            doStore(row);
                        }
                    } catch (Exception e) {
                        log.error("----> " + e);
                    }
                }
            } catch (Exception e) {
                log.error("--> " + e);
            }
        }
    }

    public void doStore(Document row) {
        mongo.getDatabase(context.getProperty("storage.mongo.database"))
                .getCollection(context.getProperty("storage.mongo.collection"))
                .replaceOne(eq("_id", row.getString("_id")), row, new UpdateOptions().upsert(true));
    }

    public void doNotify(Document row) {
        String sn = row.getString(context.getProperty("unique.field"));
        boolean needUpdate = mongo.getDatabase(context.getProperty("storage.mongo.database"))
                .getCollection(context.getProperty("storage.mongo.collection"))
                .find(regex("_id", sn + "_*")).iterator().hasNext();
        if (needUpdate) {
            log.info("Find old record in AIM, Do notify.");
            producer.send(new ProducerRecord<>(context.getProperty("publish.data.kafka.topic"), sn, row.toJson()));
        } else {
            log.info("No old record in AIM, Don't need notify.");
        }
    }
}
