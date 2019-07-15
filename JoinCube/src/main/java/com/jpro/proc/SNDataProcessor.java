package com.jpro.proc;

import com.jpro.base.JComToo;
import com.jpro.base.KafkaProxy;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;

@Log4j2
public class SNDataProcessor implements AbstractDataProcessor {

    private Properties context;

    private MongoClient mongo;

    private String mongoDatabaseName;

    public SNDataProcessor(Properties p) {
        context = p;
        dictionary = new Dictionary(p);
//        JComToo.log("\033[34;1m$$$$\033[0m SN -- MongoDB link --");
        try {
            mongo = new MongoClient(context.getProperty("mongo.server.ip"),
                    Integer.parseInt(context.getProperty("mongo.server.port")));
        } catch (Exception e) {
            log.error("Mongo link failed - " + e);
        }
//        mongo = new MongoClient(context.getProperty("mongo.server.ip"),
//                Integer.parseInt(context.getProperty("mongo.server.port")));
        mongoDatabaseName = context.getProperty("mongo.database");
        uniqueIDKey = context.getProperty("SN.unique.key");
        workOrderKey = context.getProperty("SN.work_order.key");
        modelIDKey = context.getProperty("SN.model_id.key");
        processIDKey = context.getProperty("SN.process_id.key");
        processIDValue = context.getProperty("SN.process_id.value");

        AIMCollection = context.getProperty("mongo.AIM.collection");
        AIMNotJoinedCollection = context.getProperty("mongo.AIM.unjoined.collection");

        SNCollection = context.getProperty("mongo.SN.collection");
        SNExceptionCollection = context.getProperty("mongo.SN.exception.collection");

        siteKey = context.getProperty("SN.site.key");
        siteValue = context.getProperty("SN.site.value");
        buildKey = context.getProperty("SN.build.key");
        specialBuildKey = context.getProperty("SN.special_build.key");
        productCodeKey = context.getProperty("SN.product_code.key");
        colorKey = context.getProperty("SN.color.key");
        wifi4GKey = context.getProperty("SN.wifi_4g.key");

//        JComToo.log("SNDataProcessor Constructor Over");
    }

    @Override
    public Document parse(String in) {
//        log.info("----- SN -----");
        return Document.parse(in);
    }

    private String SNExceptionCollection;
    @Override
    public void trap(String in) {
        Document row = new Document("raw", in);
        mongo.getDatabase(mongoDatabaseName).getCollection(SNExceptionCollection).insertOne(row);
    }

    private String uniqueIDKey;
    private String workOrderKey;
    private String modelIDKey;
    private String processIDKey;
    @Override
    public void validate(Document in) {
        // TODO: validate minimum requirement
        if (in.containsKey(uniqueIDKey)
                && in.get(uniqueIDKey) instanceof String
                && in.containsKey(workOrderKey)
                && in.get(workOrderKey) instanceof String
                && in.containsKey(modelIDKey)
                && in.get(modelIDKey) instanceof String  /// TODO: ????
                && in.containsKey(processIDKey)
                && in.get(processIDKey) instanceof String  /// TODO: ????
        ) {

        } else {
            throw new RuntimeException("SNDataProcessor::validate Not satisfy SN data fields requirement.");
        }
    }

    private String processIDValue;
    @Override
    public Document filter(Document in) {
        if (in.getString(processIDKey).equals(processIDValue)) {
            return in;
        }
        return null;
    }

    private Dictionary dictionary;
    private String siteKey;
    private String siteValue;
    private String buildKey;
    private String specialBuildKey;
    private String productCodeKey;
    private String colorKey;
    private String wifi4GKey;
    @Override
    public Document transform(Document in) {
        String workOrderValue = in.getString(workOrderKey);
        String buildValue = getBuildValue(workOrderValue);
        if (buildValue != null) {
            Document _r = new Document(uniqueIDKey, in.getString(uniqueIDKey))
                    .append(siteKey, siteValue)
                    .append(buildKey, buildValue)
                    .append(specialBuildKey, "");
            String modelIDValue = in.getString(modelIDKey);
            Map<String, String> extraFields = dictionary.get(modelIDValue);
            if (extraFields == null) {
                throw new RuntimeException("Extra fields not found in dictionary");
            }
            _r.put(productCodeKey, extraFields.get(productCodeKey));
            _r.put(colorKey, extraFields.get(colorKey));
            _r.put(wifi4GKey, extraFields.get(wifi4GKey));
            String mappingIDValue = _r.getString(siteKey)
                    .concat(_r.getString(productCodeKey))
                    .concat(_r.getString(colorKey))
                    .concat(_r.getString(buildKey))
                    .concat(_r.getString(specialBuildKey))
                    .concat(_r.getString(wifi4GKey));
            _r.put("mapping_id", mappingIDValue);
            return _r;
        } else {
            throw new RuntimeException("Value Of WorkOrder Not Satisfy Requirement - " + workOrderValue);
        }
        /// input:
        // 1. WORK_ORDER 2. MODEL_ID 3. SERIAL_NUMBER 4. PROCESS_ID
        // 5. PART_ID 6. MATERIAL_TYPE 7. JANCODE 8. UPCCODE

        /// output:
        // 1. SERIAL_NUMBER 2. SITE 3. PRODUCT_CODE 4. COLOR 5. BUILD 6. SPECIAL_BUILD 7. 4G_WIFI 8. mapping_id
    }
    private Pattern pattern;
    private final String regex = ".*-(.*)-.*";
    private String getBuildValue(String modelIDValue) {
        if (pattern == null) {
            pattern = Pattern.compile(regex);
        }
        Matcher matcher = pattern.matcher(modelIDValue);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null;
        }
    }

    private String AIMCollection;
    private String AIMNotJoinedCollection;
    @Override
    public Document core(Document in) {
//        log.info("----------------------core ------------------");
        String uniqueIDValue = in.getString(uniqueIDKey);
        /// TODO: store origin data first
        store(uniqueIDValue, in);

        /// TODO: process AIM data not joined
        MongoCursor<Document> joinedCursor = mongo.getDatabase(mongoDatabaseName).getCollection(AIMCollection)
                .find(regex("_id", uniqueIDValue + "*")).iterator();
        if (joinedCursor.hasNext()) {
            /// TODO: query all SN data again & do update joined AIM data
            Document SNData = getSNData(uniqueIDValue);

            while (joinedCursor.hasNext()) {
                Document ele = joinedCursor.next();
                ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
                BiConsumer<String, Object> f = (k, v) -> {
                    if (ele.containsKey(k)) return;
                    lock.writeLock().lock();
                    ele.put(k, v);
                    lock.writeLock().unlock();
                };
                SNData.forEach(f);
                mongo.getDatabase(mongoDatabaseName).getCollection(SNCollection)
                        .findOneAndUpdate(eq("_id", ele.getString("_id")), new Document("$set", ele));
            }
        } else {
            MongoCursor<Document> unjoinedCursor = mongo.getDatabase(mongoDatabaseName).getCollection(AIMNotJoinedCollection)
                    .find(regex("_id", uniqueIDValue + "*")).iterator();
            if (unjoinedCursor.hasNext()) {
                /// TODO: collect AIM/CNC/SN data, do second join if necessary
                Document SNData = getSNData(uniqueIDValue);
                if (SNData == null) {
                    log.error("Impossible !!! CNC cannot be empty, origin data - " + in.toJson());
                    return in;
                }

                while (unjoinedCursor.hasNext()) {
                    Document ele = unjoinedCursor.next();
                    ele.put("SN", SNData);
                    // TODO: validate the AIM data, if it has 'SN' & 'CNC' / 'SPM', do second join
                    Document secondJoinData = AIMDataProcessor.doSecondJoin(ele);
                    if (secondJoinData != null) {
                        mongo.getDatabase(mongoDatabaseName).getCollection(AIMNotJoinedCollection)
                                .findOneAndDelete(eq("_id", ele.getString("_id")));
                        secondJoinData.put("_id", ele.getString("_id"));
                        mongo.getDatabase(mongoDatabaseName).getCollection(AIMCollection)
                                .replaceOne(eq("_id", ele.getString("_id")), secondJoinData, new UpdateOptions().upsert(true));
                    } else {
                        mongo.getDatabase(mongoDatabaseName).getCollection(AIMNotJoinedCollection)
                                .replaceOne(eq("_id", ele.getString("_id")), ele, new UpdateOptions().upsert(true));
//                                .findOneAndUpdate(eq("_id", ele.getString("_id")), new Document("$set", ele));
                    }
                }
            }
        }
        return null;
    }
    private Document getSNData(String uniqueIDValue) {
        return getSNData(uniqueIDValue, this.mongo);
    }
    public static Document getSNData(String uniqueIDValue, MongoClient mongo) {
        MongoCursor<Document> mongoCursor =
                mongo.getDatabase(JComToo.T().getMongoDatabase()).getCollection(JComToo.T().getSNCollection())
                        .find(regex("_id", uniqueIDValue)).iterator();
        if (mongoCursor.hasNext()) {
            return mongoCursor.next();
        } else {
            return null;
        }
    }
    private String SNCollection;
    private void store(String id, Document row) {
        row.put("_id", id);
        mongo.getDatabase(mongoDatabaseName).getCollection(SNCollection)
                .replaceOne(eq("_id", id), row, new ReplaceOptions().upsert(true));;
    }

    @Override
    public void post(Document in) {

    }

    public KafkaConsumer<String, String> makeKafkaConsumer() {
        Properties conProps = KafkaProxy.makeBaseAttr();
        conProps.put("bootstrap.servers", context.getProperty("kafka.server.url"));
        conProps.put("group.id", context.getProperty("SN.group.id"));
        KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(conProps);
        consumer.subscribe(Collections.singleton(context.getProperty("SN.topic")));
        return consumer;
    }
}
