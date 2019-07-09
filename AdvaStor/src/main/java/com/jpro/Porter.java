package com.jpro;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    private AbstDataProc dataProc;

    Porter(Properties pro) {
        context = pro;
    }

    private void prepare() {
        if (context.getProperty("mode").toLowerCase().equals("cnc")) {
            dataProc = new CNCDataProc(context);
        } else if (context.getProperty("mode").toLowerCase().equals("spm")) {
            dataProc = new SPMDataProc(context);
        } else if (context.getProperty("mode").toLowerCase().equals("sn")) {
            dataProc = new SNDataProc(context);
        } else {
            dataProc = new SimpDataProc(context);
        }
        if (context.getProperty("filter.switch").toLowerCase().equals( "true")) {
            needFilter = true;
        }
        needNotify = context.getProperty("notify.switch").toLowerCase().equals("true");
        if (context.getProperty("notify.switch").toLowerCase().equals("false")) needNotify = false;
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
        log.info("Wait Kafka ...");
        while (isWorking) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.parseLong(context.getProperty("kafka.timeout"))));
                if (records.count() == 0) continue;
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Document row;
                        try {
                            row = Document.parse(record.value());
                        } catch (Exception e) {
                            log.error("Parse data failed - " + e);
                            doStoreExceptionData(new Document("raw", record.value()));
                            continue;
                        }

                        try {
                            dataProc.doFilter(row);
                        } catch (Exception e) {
                            log.error("Check data error - " + e);
                            doStoreExceptionData(Document.parse(record.value()));
                            continue;
                        }

                        if (needFilter) {
                            try {
                                row = dataProc.doFilter(row);
                            } catch (Exception e) {
                                log.error("Do filter error - " + e);
                                doStoreExceptionData(Document.parse(record.value()));
                                continue;
                            }
                            if (row.isEmpty()) continue;
                        }

                        if (row != null && !row.isEmpty()) {
                            Document storageData;
                            try {
                                storageData = dataProc.generateStorageData(row);
                            } catch (Exception e) {
                                log.error("Do generate storage data error - " + e);
                                doStoreExceptionData(Document.parse(record.value()));
                                continue;
                            }
                            doStore(storageData);

                            // TODO: notify
                        /*    if (needNotify) {
                                // 1.
                                boolean ensureDo = doQuery(sn);

                                if (ensureDo) {
                                    // 2.
                                    notifyData = generateNotifyData(row);

                                    // 3.
                                    doNotify(notifyData);
                                }
                            }*/

                            if (needNotify) {
                                Document notifyData;
                                try {
                                    notifyData = dataProc.generateNotifyData(row);
                                } catch (Exception e) {
                                    log.error("Do reshape error - " + e);
                                    doStoreExceptionData(Document.parse(record.value()));
                                    continue;
                                }
                                doNotify(notifyData);
                            }
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

    private void doStore(Document row) {
        mongo.getDatabase(context.getProperty("storage.mongo.database"))
                .getCollection(context.getProperty("storage.mongo.collection"))
                .replaceOne(eq("_id", row.getString("_id")), row, new UpdateOptions().upsert(true));
        log.info("Porter::doStore complete");
    }

    private void doStoreExceptionData(Document row) {
        row.remove("_id");
        mongo.getDatabase(context.getProperty("storage.mongo.database"))
                .getCollection(context.getProperty("storage.exception.collection"))
                .insertOne(row);
        log.info("Porter::doStoreExceptionData complete");
    }

//    private List<Document> doQuery(String uni) {
//        List<Document> _r = new ArrayList<>();
//        for (Document document : mongo.getDatabase(context.getProperty("storage.mongo.database"))
//                .getCollection(context.getProperty("storage.aim.collection"))
//                .find(regex("_id", uni + "_*"))) {
//            _r.add(document);
//        }
//        return _r;
//    }

    private void doNotify(Document row) {
        row.forEach((k, v) -> {
            log.info(k + " - " + v);
        });
        String sn = row.getString(context.getProperty("unique.field"));
        log.info("sn: " + sn);
        if (sn == null) {
            log.error("{} is null! please check properties file.", context.getProperty("unique.field"));
            return;
        }
        MongoCursor mongoCursor = mongo.getDatabase(context.getProperty("storage.mongo.database"))
                .getCollection(context.getProperty("storage.aim.collection"))
                .find(regex("_id", sn + "_*")).iterator();
        boolean needUpdate = mongoCursor.hasNext();
        if (needUpdate) {
            log.info("Find old record in AIM, Do notify.");
            producer.send(new ProducerRecord<>(context.getProperty("notify.kafka.topic"), sn, row.toJson()));
            log.info("Published key [ " + sn + " ] ; value [ " + row.toJson() + " ].");
        } else {
            log.info("No old record in AIM, Don't need notify.");
        }
    }
}
