package com.jpro;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.text.ParseException;
import java.time.Duration;
import java.util.*;

import static com.mongodb.client.model.Filters.*;

@Log4j2
public class Joinner {
    private boolean isWorking = false;

    /**
     * The necessary external injection
     */
    private Properties context;

    /**
     * Construct by self
     */
    private MongoClient mongoOfSN;
    private MongoClient mongoOfCNC;
    private MongoClient mongoOfSPM;
    private MooPoo mongosOfAIM;

    private KafkaConsumer<String, String> consumerOfAIM;

    private Map<String, SpcCon> spcConfigTable;

    Joinner(Properties pro, MooPoo poo) {
        context = pro;
        mongosOfAIM = poo;
    }

    public void stop() {
        isWorking = false;
    }

    private void prepare() {
        spcConfigTable = ComToo.generateSpcConfig(context.getProperty("spc.config"));
        /// Database storage settings
        mongoOfSN = new MongoClient(context.getProperty("storage.mongo.ip"),
                Integer.parseInt(context.getProperty("storage.mongo.port")));
        mongoOfCNC = new MongoClient(context.getProperty("storage.mongo.ip"),
                Integer.parseInt(context.getProperty("storage.mongo.port")));
        mongoOfSPM = new MongoClient(context.getProperty("storage.mongo.ip"),
                Integer.parseInt(context.getProperty("storage.mongo.port")));
        /// Data source
        Properties conProps = new Properties();
        conProps.put("bootstrap.servers", context.getProperty("source.kafka.url"));
        conProps.put("group.id", context.getProperty("source.group.id"));
        conProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerOfAIM = new org.apache.kafka.clients.consumer.KafkaConsumer<>(conProps);
        consumerOfAIM.subscribe(Collections.singleton(context.getProperty("source.kafka.topic")));
    }

    public void work() {
        prepare();
        isWorking = true;
        log.info("Wait Kafka ...");
        while (isWorking) {
            try {
                ConsumerRecords<String, String> records = consumerOfAIM.poll(
                        Duration.ofMillis(Long.parseLong(context.getProperty("kafka.timeout"))));
                if (records.count() == 0) continue;
                log.info("Process data batch: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // TODO: step 1 parse
                        Document row;
                        try {
                            row = Document.parse(record.value());
                        } catch (Exception e) {
                            log.error("Parse AIM data failed - " + record.value());
                            doStoreExceptionData(new Document("raw", record.value()));
                            continue;
                        }
                        log.info("Complete parse AIM data");

                        // TODO: step 2 check
                        try {
                            checkConditionOfAIM(row);
                        } catch (Exception e) {
                            log.error(e);
                            doStoreExceptionData(row);
                            continue;
                        }
                        log.info("Check AIM data");

                        // TODO: step generate TIME_2 TIME_3 fields
                        generateTimeFieldsForWangDong(row);

                        // TODO: step 3 append parse spc data
                        shapeSPC(row);

                        // TODO: step 4 query SN & CNC / SPM
                        Document inc1 = queryFromSN(row.getString(context.getProperty("unique.key")));

                        Document inc2;
                        if (row.getString(context.getProperty("process.key")).toLowerCase().equals("fqc")) {
                            log.info("==> AIM ++ SPM");
                            inc2 = queryFromSPM(row.getString(context.getProperty("unique.key")));
                        } else if (row.getString(context.getProperty("process.key")).toLowerCase().equals("laser_qc")) {
                            log.info("==> AIM ++ CNC");
                            inc2 = queryFromCNC_v2(row.getString(context.getProperty("unique.key")));
                        } else if (row.getString(context.getProperty("process.key")).toLowerCase().contains("laser_qc")) {
                            log.info("==> AIM ++ CNC (Contains Version)");
                            inc2 = queryFromCNC_v2(row.getString(context.getProperty("unique.key")));
                        } else {
                            log.info("==> AIM ++ -");
                            inc2 = new Document();
                        }

                        // TODO: step 5 merge
                        inc1.forEach((k, v) -> {
                            if (!k.equals(context.getProperty("unique.key"))) {
                                row.put(k, v);
                            }
                        });
                        inc2.forEach((k, v) -> {
                            if (!k.equals(context.getProperty("unique.key"))) {
                                row.put(k, v);
                            }
                        });
                        if (!inc1.isEmpty()) {
                            generateMappingIDForWangDong(row);
                        }
                        log.info("Merge");

                        // TODO: final store
                        finalStore("_id", row.getString(context.getProperty("unique.key"))
                                + "_" + row.getString(context.getProperty("station.key")), row);
                        log.info("Store");
                    } catch (Exception e) {
                        log.error("----> " + e + " -> " + record.value());
                    }
                }
            } catch (Exception e) {
                log.error("--> " + e);
            }
        }
        log.info("Exiting Joinner work.");
    }

    private Document generateTimeFieldsForWangDong(Document orig) {
        if (!orig.containsKey(context.getProperty("aim.time1.key"))) {
            return orig;
        }
        if (orig.get(context.getProperty("aim.time1.key")) instanceof String) {
            String timeValue = orig.getString(context.getProperty("aim.time1.key"));
            try {
                long timeLong;
                if (timeValue.charAt(4) == '-' && timeValue.charAt(7) == '-' && timeValue.charAt(10) == ' ') {
                    timeLong = ComToo.timestampToLong(timeValue, "yyyy-MM-dd HH:mm:ss");
                } else if (timeValue.charAt(4) == '/' && timeValue.charAt(7) == '/' && timeValue.charAt(10) == ' ') {
                    timeValue = timeValue.substring(0, 4) + "-" + timeValue.substring(5, 2) + "-" + timeValue.substring(8, 11);
                    timeLong = ComToo.timestampToLong(timeValue, "yyyy-MM-dd HH:mm:ss");
                } else {
                    log.error("unknown time format in AIM data.");
                    return orig;
                }
                orig.put(context.getProperty("aim.time1.key"), timeLong);
                orig.put(context.getProperty("aim.time2.key"), (int)(timeLong / 1000 / 60 / 60));
                orig.put(context.getProperty("aim.time3.key"), (int)(timeLong / 1000 / 60 / 60 / 24));
            } catch (ParseException pe) {
                log.error("unknown time format in AIM data.");
                return orig;
            }
        } else if (orig.get(context.getProperty("aim.time1.key")) instanceof Long) {
            long timeLong = orig.getLong(context.getProperty("aim.time1.key"));
            orig.put(context.getProperty("aim.time1.key"), timeLong);
            orig.put(context.getProperty("aim.time2.key"), (int)(timeLong / 1000 / 60 / 60));
            orig.put(context.getProperty("aim.time3.key"), (int)(timeLong / 1000 / 60 / 60 / 24));
        }
        return orig;
    }

    private void generateMappingIDForWangDong(Document orig) {
        if (!orig.containsKey(context.getProperty("aim.site.key"))
                || !orig.containsKey(context.getProperty("aim.source.key"))
                || !orig.containsKey(context.getProperty("sn.color.key"))
                || !orig.containsKey(context.getProperty("sn.builds.key"))
                || !orig.containsKey(context.getProperty("sn.special_build.key"))
                || !orig.containsKey(context.getProperty("sn.wifi_4g.key"))
        ) {
            log.warn("lack of necessary field in generating MappingID field!");
            return;
        }
        StringBuilder mappingID = new StringBuilder();
        mappingID.append(orig.getString(context.getProperty("aim.site.key")))
                .append(orig.getString(context.getProperty("aim.source.key")))
                .append(orig.getString(context.getProperty("sn.color.key")))
                .append(orig.getString(context.getProperty("sn.builds.key")))
                .append(orig.getString(context.getProperty("sn.special_build.key")))
                .append(orig.getString(context.getProperty("sn.wifi_4g.key")));
        orig.put("mapping_id", mappingID.toString());
    }

    private SpcCon queryFromConfig(String spcName, String staionType) {
        // TODO: read config file
        String key = spcName + ":" + staionType;
        return spcConfigTable.getOrDefault(key, null);
    }

    private void shapeSPC(Document row) {
        //Document spc = new Document();
        List<Document> spcs = new ArrayList<>();
        row.forEach((key, val) -> {
            if (key.contains("SPC") && row.get(key) instanceof String) {
                try {
                    double spcValue = Double.parseDouble((String) val);
                    SpcCon spcCon = queryFromConfig(key.substring(4, key.length() - 4), row.getString(context.getProperty("station.key")));
                    if (spcCon == null) {
                        log.warn("Cannot find low_limit - norminal - up_limit from config file.");
                        return;
                    }
                    Document oneOfSpc = new Document();
                    oneOfSpc.put("NAME", key);
                    oneOfSpc.put("VALUE", spcValue);
                    if (spcValue >= spcCon.getDownlimit() && spcValue <= spcCon.getUplimit()) {
                        oneOfSpc.put("GOOD", true);
                    } else {
                        oneOfSpc.put("GOOD", false);
                    }
                    oneOfSpc.put("up_limit", spcCon.getUplimit());
                    oneOfSpc.put("low_limit", spcCon.getDownlimit());
                    oneOfSpc.put("norminal", spcCon.getNormal());
                    spcs.add(oneOfSpc);
                    row.remove(key);
                } catch (Exception e) {
                    log.error("Joinner::shapeSPC -> " + e);
                }
            }
        });
        row.put("SPC", spcs);
    }

    private void finalStore(String key, String val, Document row) {
        row.remove("_id");
        MongoClient mongo = mongosOfAIM.getMongoClient();
        ComToo.upsertMongo(mongo, context.getProperty("storage.mongo.database"), context.getProperty("storage.aim.collection"), key, val, row);
        mongosOfAIM.returnMongoClient(mongo);
    }

    private Document queryFromSN(String unique) {
        Document res = ComToo.findOneMongo(mongoOfSN, context.getProperty("storage.mongo.database"), context.getProperty("storage.sn.collection"), "_id", unique);
        if (res == null) return new Document();
        return res;
    }

    private Document queryFromSPM(String unique) {
        Document res = new Document();

        return res;
    }

    private Document queryFromCNC_v2(String unique) {
        List<Document> recordsOfCNC = new ArrayList<>();
        MongoCursor<Document> mongoCursor = mongoOfCNC.getDatabase(context.getProperty("storage.mongo.database")).getCollection(context.getProperty("storage.cnc.collection"))
                .find(regex("_id", unique + "_*")).iterator();
        while (mongoCursor.hasNext()) {
            Document row = mongoCursor.next();
            if (!row.containsKey(context.getProperty("cnc.process.field"))
                    || !row.containsKey(context.getProperty("cnc.cell.field"))
                    || !row.containsKey(context.getProperty("cnc.machine.field"))) {
                log.warn("lack necessary field in CNC data, continue join others");
                continue;
            }
            Document prow = new Document();
            prow.put(context.getProperty("cnc.process.replace"), row.getString(context.getProperty("cnc.process.field")));
            prow.put(context.getProperty("cnc.cell.replace"), row.getString(context.getProperty("cnc.cell.field")));
            prow.put(context.getProperty("cnc.machine.field"), row.getString(context.getProperty("cnc.machine.replace")));
            recordsOfCNC.add(prow);
        }
        return new Document("CNC", recordsOfCNC);
    }

    private Document queryFromCNC(String unique) {
        List<Document> li = new ArrayList<>();
        Document res = new Document();
        log.info("sn number: " + unique);
        Document res1 = ComToo.findOneMongo(mongoOfCNC, context.getProperty("storage.mongo.database"), context.getProperty("storage.cnc.collection"), "_id", unique + "_CNC7");
        Document res2 = ComToo.findOneMongo(mongoOfCNC, context.getProperty("storage.mongo.database"), context.getProperty("storage.cnc.collection"), "_id", unique + "_CNC8");
        Document res3 = ComToo.findOneMongo(mongoOfCNC, context.getProperty("storage.mongo.database"), context.getProperty("storage.cnc.collection"), "_id", unique + "_CNC9");
        Document res4 = ComToo.findOneMongo(mongoOfCNC, context.getProperty("storage.mongo.database"), context.getProperty("storage.cnc.collection"), "_id", unique + "_CNC10");
        if (res1 != null) li.add(res1);
        if (res2 != null) li.add(res2);
        if (res3 != null) li.add(res3);
        if (res4 != null) li.add(res4);
        if (li.size() == 0) return new Document();
        li.forEach((doc) -> {
            if (!doc.containsKey("PROCESS_NAME") || !doc.containsKey("CELL") || !doc.containsKey("MACHINE_NAME")) {
                log.warn("Lack of PROCESS_NAME | CELL | MACHINE_NAME");
                return;
            }
            String processName = doc.getString("PROCESS_NAME");
            if (processName.contains("CNC7")) {
                res.append("CNC7_NAME", processName);
                res.append("CNC7_CELL", doc.getString("CELL"));
                res.append("CNC7_MC", doc.getString("MACHINE_NAME"));
            } else if (processName.contains("CNC8")) {
                res.append("CNC8_NAME", processName);
                res.append("CNC8_CELL", doc.getString("CELL"));
                res.append("CNC8_MC", doc.getString("MACHINE_NAME"));
            } else if (processName.contains("CNC9")) {
                res.append("CNC9_NAME", processName);
                res.append("CNC9_CELL", doc.getString("CELL"));
                res.append("CNC9_MC", doc.getString("MACHINE_NAME"));
            } else if (processName.contains("CNC10")) {
                res.append("CNC10_NAME", processName);
                res.append("CNC10_CELL", doc.getString("CELL"));
                res.append("CNC10_MC", doc.getString("MACHINE_NAME"));
            }
        });

        return res;
    }

    private void checkConditionOfAIM(Document row) {
        if (!row.containsKey(context.getProperty("unique.key"))) {
            throw new RuntimeException("No " + context.getProperty("unique.key") + " in AIM data.");
        }
        if (!row.containsKey(context.getProperty("station.key"))) {
            throw new RuntimeException("No " + context.getProperty("station.key" + " in AIM data."));
        }
        if (!row.containsKey(context.getProperty("process.key"))) {
            throw new RuntimeException("No " + context.getProperty("process.key" + " in AIM data."));
        }
    }

    private void doStoreExceptionData(Document row) {
        row.remove("_id");
        MongoClient mongo = mongosOfAIM.getMongoClient();
        try {
            mongo.getDatabase(context.getProperty("storage.mongo.database"))
                    .getCollection(context.getProperty("storage.exc.collection"))
                    .insertOne(row);
        } catch (Exception e) {
            log.error(e);
        }
        mongosOfAIM.returnMongoClient(mongo);
        log.info("Porter::doStoreExceptionData complete");
    }
}
