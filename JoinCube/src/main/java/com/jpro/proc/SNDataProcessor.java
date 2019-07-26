package com.jpro.proc;

import com.jpro.base.*;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Log4j2
public class SNDataProcessor implements AbstractDataProcessor {

    private Properties context;

    private MongoClient mongo;

    public SNDataProcessor(Properties p) {
        context = p;
        dictionary = new Dictionary(p);
        try {
            mongo = new MongoClient(context.getProperty("mongo.server.ip"),
                    Integer.parseInt(context.getProperty("mongo.server.port")));
        } catch (Exception e) {
            log.error("Mongo link failed - " + e);
        }
        workOrderKey            = context.getProperty("SN.work_order.key");
        modelIDKey              = context.getProperty("SN.model_id.key");
        processIDKey            = context.getProperty("SN.process_id.key");
        processIDValue          = context.getProperty("SN.process_id.value");

        SNExceptionCollection   = context.getProperty("mongo.SN.exception.collection");

        siteKey                 = context.getProperty("SN.site.key");
        siteValue               = context.getProperty("SN.site.value");
        buildKey                = context.getProperty("SN.build.key");
        specialBuildKey         = context.getProperty("SN.special_build.key");
        productCodeKey          = context.getProperty("SN.product_code.key");
        colorKey                = context.getProperty("SN.color.key");
        wifi4GKey               = context.getProperty("SN.wifi_4g.key");
    }

    @Override
    public Document parse(String in) {
        return Document.parse(in);
    }

    private String SNExceptionCollection;
    @Override
    public void trap(String in) {
        Document row = new Document("raw", in);
        mongo.getDatabase(GlobalContext.mongoDatabase).getCollection(SNExceptionCollection).insertOne(row);
    }

    private String workOrderKey;
    private String modelIDKey;
    private String processIDKey;
    @Override
    public void validate(Document in) {
        // TODO: validate minimum requirement
        if (in.containsKey(GlobalContext.uniqueIdKeyOfSn)
                && in.get(GlobalContext.uniqueIdKeyOfSn) instanceof String
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
            Document _r = new Document(GlobalContext.uniqueIdKeyOfSn, in.getString(GlobalContext.uniqueIdKeyOfSn))
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

    @Override
    public Document core(Document in) {
        String uniqueIDValue = in.getString(GlobalContext.uniqueIdKeyOfSn);
        /// TODO: store origin data first
        MongoProxy.upsertToSn(in);

        /// TODO: use bloom filter
        if (!GlobalBloom.getFilterForAIM().mightContain(uniqueIDValue)
                && !GlobalBloom.getFilterForNotJoin().mightContain(uniqueIDValue)) {
            return in;
        }

        /// TODO: process AIM data not joined
        MongoCursor<Document> joinedCursor = MongoProxy.queryFromAim(uniqueIDValue, mongo);

        if (joinedCursor.hasNext()) {
            log.warn("Rare branch, update unique ID.");
            /// TODO: query all SN data again & do update joined AIM data
            while (joinedCursor.hasNext()) {
                log.info("Rare branch, update unique ID! - " + in.toJson());
                Document ele = joinedCursor.next();
                JComToo.gatherDocument(ele, in);
                MongoProxy.upsertToAim(ele);
            }
        } else {
            MongoCursor<Document> unjoinedCursor = MongoProxy.queryFromUnjoined(uniqueIDValue, mongo);

            if (unjoinedCursor.hasNext()) {
                log.warn("Sequence problem, unique ID must first come in, in theory.");
                /// TODO: collect AIM/CNC/SN data, do second join if necessary
                while (unjoinedCursor.hasNext()) {
                    Document ele = unjoinedCursor.next();
                    ele.put("SN", in);
                    // TODO: validate the AIM data, if it has 'SN' & 'CNC' / 'SPM', do second join
                    AIMDataProcessor.processSecondJoin(uniqueIDValue, ele, mongo);
                }
            }
        }
        return null;
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
