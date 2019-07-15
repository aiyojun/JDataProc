package com.jpro.proc;

import com.jpro.base.JComToo;
import com.jpro.base.MongoSingle;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

import static com.mongodb.client.model.Filters.*;

@Log4j2
public class AIMDataProcessor implements AbstractDataProcessor {

    private Properties context;

    private MongoClient mongo;

    public AIMDataProcessor(Properties p) {
        context = p;
        try {
            mongo = new MongoClient(context.getProperty("mongo.server.ip"),
                    Integer.parseInt(context.getProperty("mongo.server.port")));
        } catch (Exception e) {
            log.error("Mongo link failed - " + e);
        }
        mongoDatabaseName       = context.getProperty("mongo.database");
        AIMCollection           = context.getProperty("mongo.AIM.collection");
        AIMNotJoinedCollection  = context.getProperty("mongo.AIM.unjoined.collection");
        AIMExceptionCollection  = context.getProperty("mongo.AIM.exception.collection");

        /// TODO: inner varibles preparations
        timeIncrement       = JComToo.T().parseTimeDiff(context.getProperty("AIM.time.diff").toLowerCase()) * 60 * 60 * 1000;
        uniqueIDKey         = context.getProperty("AIM.unique.key");
        timestampKey        = context.getProperty("AIM.timestamp.key");
        passKey             = context.getProperty("AIM.pass.key");
        dataKey             = context.getProperty("AIM.data.key");
        resultsKey          = context.getProperty("AIM.results.key");
        measurementsKey     = context.getProperty("AIM.measurements.key");
        stationKey          = context.getProperty("AIM.station.key");
        processKey          = context.getProperty("AIM.process.key");
        processValueToCNC   = context.getProperty("AIM.process.value1");
        processValueToSPM   = context.getProperty("AIM.process.value2");

        time1Key            = context.getProperty("AIM.time1.key");
        time2Key            = context.getProperty("AIM.time2.key");
        time3Key            = context.getProperty("AIM.time3.key");
        spcValueKey         = context.getProperty("AIM.spc.value.key");
        spcUpperLimitKey    = context.getProperty("AIM.spc.upperlimit.key");
        spcLowerLimitKey    = context.getProperty("AIM.spc.lowerlimit.key");
        spcPassKey          = context.getProperty("AIM.spc.pass.key");
    }

    @Override
    public Document parse(String in) {
//        log.info("----- AIM -----");
        return Document.parse(in);
    }

    private String AIMExceptionCollection;
    @Override
    public void trap(String in) {
        Document row = new Document("raw", in);
        mongo.getDatabase(mongoDatabaseName).getCollection(AIMExceptionCollection).insertOne(row);
    }

    private String uniqueIDKey;
    private String timestampKey;
    private String passKey;
    private String dataKey;
    private String resultsKey;
    private String stationKey;
    private String processKey;
    private String measurementsKey;
    @Override
    public void validate(Document in) {
        // TODO: validate minimum requirement
        if (in.containsKey(uniqueIDKey) && in.get(uniqueIDKey) instanceof String
                && in.containsKey(timestampKey)
                && in.get(timestampKey) instanceof String
                && in.containsKey(passKey) && in.get(passKey) instanceof Boolean
                && in.containsKey(dataKey) && in.get(dataKey) instanceof Document
                && in.get(dataKey, Document.class).containsKey(resultsKey)
                && in.get(dataKey, Document.class).get(resultsKey) instanceof Document
                && in.get(dataKey, Document.class).get(resultsKey, Document.class).containsKey(stationKey)
                && in.get(dataKey, Document.class).get(resultsKey, Document.class).get(stationKey) instanceof String
                && in.get(dataKey, Document.class).get(resultsKey, Document.class).containsKey(processKey)
                && in.get(dataKey, Document.class).get(resultsKey, Document.class).get(processKey) instanceof String
                && in.get(dataKey, Document.class).get(resultsKey, Document.class).containsKey(measurementsKey)
                && in.get(dataKey, Document.class).get(resultsKey, Document.class).get(measurementsKey) instanceof List
        ) {

        } else {
            throw new RuntimeException("AIMDataProcessor::validate Not satisfy minimum fields requirement.");
        }
    }

    @Override
    public Document filter(Document in) {
        return in;
    }

    private long timeIncrement;
    private String time1Key;
    private String time2Key;
    private String time3Key;
    private String spcValueKey;
    private String spcUpperLimitKey;
    private String spcLowerLimitKey;
    private String spcPassKey;
    @Override
    public Document transform(Document in) {
        Document _r = new Document(uniqueIDKey, in.getString(uniqueIDKey))
                .append(timestampKey, in.getString(timestampKey))
                .append(passKey, in.getBoolean(passKey));
        /// TODO: only process timestamp like "2019-07-12T06:16:00Z"
        String timeVal = in.getString(timestampKey);
        timeVal = timeVal.toUpperCase();
        if ((timeVal.length() == 19 || timeVal.length() == 20) && timeVal.charAt(4) == '-' && timeVal.charAt(7) == '-'
                && timeVal.charAt(10) == 'T' && timeVal.charAt(13) == ':' && timeVal.charAt(16) == ':') {
            StringBuilder sb = new StringBuilder()
                    .append(timeVal, 0, 10).append(" ")
                    .append(timeVal, 11, 19);
            try {
                Date realDateOfThatDay = new Date(JComToo.T().timestampToDate(sb.toString()).getTime() +
                        ((timeVal.length() == 20 && timeVal.charAt(19) == 'Z') ? timeIncrement : 0));
                String sRealDateOfThatDay = JComToo.T().toTimestamp(realDateOfThatDay);
                String sHourOfThatDay = sRealDateOfThatDay.substring(0, 14).concat("00:00");
                String sDaysOfThatDay = sRealDateOfThatDay.substring(0, 11).concat("00:00:00");
                long time_1 = realDateOfThatDay.getTime();
                long time_2 = JComToo.T().timestampToDate(sHourOfThatDay).getTime();
                long time_3 = JComToo.T().timestampToDate(sDaysOfThatDay).getTime();
                _r.put(time1Key, time_1);
                _r.put(time2Key, time_2);
                _r.put(time3Key, time_3);
            } catch (ParseException e) {
                throw new RuntimeException("Parse date failed [ " + timeVal + " ]");
            }
        } else {
            throw new RuntimeException("Incorrect timestamp format.");
        }
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        Document results = in.get(dataKey, Document.class).get(resultsKey, Document.class);
        results.forEach((k, v) -> {
            /// TODO: append SPC pass field, just increment
            if (k.equals(measurementsKey)) {
                List<Document> spcInfos = results.getList(measurementsKey, Document.class);
                for (Document ele : spcInfos) {
                    if (ele.containsKey(spcValueKey)
                            && (ele.get(spcValueKey) instanceof Integer || ele.get(spcValueKey) instanceof Double || ele.get(spcValueKey) instanceof Long)
                            && ele.containsKey(spcUpperLimitKey)
                            && (ele.get(spcUpperLimitKey) instanceof Integer || ele.get(spcUpperLimitKey) instanceof Double || ele.get(spcUpperLimitKey) instanceof Long)
                            && ele.containsKey(spcLowerLimitKey)
                            && (ele.get(spcLowerLimitKey) instanceof Integer || ele.get(spcLowerLimitKey) instanceof Double || ele.get(spcLowerLimitKey) instanceof Long)
                    ) {
                        ele.put(spcPassKey, Double.parseDouble(ele.get(spcValueKey).toString()) <= Double.parseDouble(ele.get(spcUpperLimitKey).toString())
                                && Double.parseDouble(ele.get(spcValueKey).toString()) >= Double.parseDouble(ele.get(spcLowerLimitKey).toString()));
                    }
                }
            }
            if (k.equals(uniqueIDKey) || k.equals(timestampKey) || k.equals(passKey)
                    || k.equals(time1Key) || k.equals(time2Key) || k.equals(time3Key)) {
                return;
            }
            lock.writeLock().lock();
            _r.put(k, v);
            lock.writeLock().unlock();
        });

        return _r;
    }

    /**
     * Do join & Store joined data in here
     */
    private String processValueToCNC;
    private String processValueToSPM;
    @Override
    public Document core(Document in) {
        Document _r = null;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        String uniqueIDValue = in.getString(uniqueIDKey);
        String stationValue = in.getString(stationKey);
        String processValue = in.getString(processKey);
        String id = in.getString(uniqueIDKey) + processValue + stationValue;
        /// share function
        BiConsumer<String, Object> f = (k, v) -> {
            if (in.containsKey(k)) return;
            lock.writeLock().lock();
            in.put(k, v);
            lock.writeLock().unlock();
        };
        if (processValue.toLowerCase().equals(processValueToCNC.toLowerCase())) {
            Document SNNData = queryFromSNN(uniqueIDValue);
            Document CNCData = CNCDataProcessor.getCNCData(uniqueIDValue, mongo);
//            log.info("----------0 snn: " + uniqueIDValue);
//            log.info("----------1 snn: " + SNNData.toJson());
            if ((SNNData != null && !SNNData.isEmpty()) && (CNCData != null && !CNCData.isEmpty())) {
//            log.info("----------2");
                SNNData.forEach(f);
                CNCData.forEach(f);
                storeJoinedData(id, in);
                _r = in;
            } else if ((SNNData == null || SNNData.isEmpty()) && (CNCData == null || CNCData.isEmpty())) {
//            log.info("----------3");
                _r = new Document("AIM", in);
                storeNotJoinedData(id, _r);
            } else if ((SNNData == null || SNNData.isEmpty()) && (CNCData != null && !CNCData.isEmpty())) {
//            log.info("----------4");
                _r = new Document("AIM", in).append("CNC", CNCData);
                storeNotJoinedData(id, _r);
            } else if ((SNNData != null || !SNNData.isEmpty()) && (CNCData == null || CNCData.isEmpty())) {
//            log.info("----------5");
                _r = new Document("AIM", in).append("SN", SNNData);
                storeNotJoinedData(id, _r);
            }
//            log.info("----------6");
        } else if (processValue.toLowerCase().equals(processValueToSPM.toLowerCase())) {
//            Document SNNData = queryFromSNN(uniqueIDValue);
//            Document SPMData = queryFromSPM(uniqueIDValue);
//            if ((SNNData != null && !SNNData.isEmpty()) && (SPMData != null && !SPMData.isEmpty())) {
//                SNNData.forEach(f);
//                SPMData.forEach(f);
//                storeJoinedData(id, in);
//                _r = in;
//            } else if ((SNNData == null || SNNData.isEmpty()) && (SPMData == null || SPMData.isEmpty())) {
//                _r = new Document("AIM", in);
//                storeNotJoinedData(id, _r);
//            } else if ((SNNData == null || SNNData.isEmpty()) && (SPMData != null && !SPMData.isEmpty())) {
//                _r = new Document("AIM", in).append("SPM", SPMData);
//                storeNotJoinedData(id, _r);
//            } else if ((SNNData != null || !SNNData.isEmpty()) && (SPMData == null || SPMData.isEmpty())) {
//                _r = new Document("AIM", in).append("SN", SNNData);
//                storeNotJoinedData(id, _r);
//            }
        } else {
            log.warn("AIM Join With ??? " + processKey + " : " + processValue);
        }

        return _r;
    }

    /**
     * Do some post debug, validate the final storage data
     */
    @Override
    public void post(Document in) {
//        if (in != null) {
//            log.info("Joinner - " + this.hashCode() + " - " + in.toJson());
//        }
    }

    public static Document doSecondJoin(Document in) {
        if (in.containsKey("CNC") && in.containsKey("SN") && in.containsKey("AIM")) {
            Document row = new Document();
            Document cnc = in.get("CNC", Document.class);
            Document snn = in.get("SN" , Document.class);
            Document aim = in.get("AIM", Document.class);
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            BiConsumer<String, Object> f = (k, v) -> {
                if (row.containsKey(k)) return;
                lock.writeLock().lock();
                row.put(k, v);
                lock.writeLock().unlock();
            };
            aim.forEach(f);
            cnc.forEach(f);
            snn.forEach(f);
            return row;
        } else if (in.containsKey("SPM") && in.containsKey("SN") && in.containsKey("AIM")) {
            Document row = new Document();
            Document spm = in.get("SPM", Document.class);
            Document snn = in.get("SN" , Document.class);
            Document aim = in.get("AIM", Document.class);
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            BiConsumer<String, Object> f = (k, v) -> {
                if (row.containsKey(k)) return;
                lock.writeLock().lock();
                row.put(k, v);
                lock.writeLock().unlock();
            };
            aim.forEach(f);
            spm.forEach(f);
            snn.forEach(f);
            return row;
        }
        return null;
    }

    private Document queryFromSNN(String sn) {
        MongoCursor<Document> mongoCursor =
                mongo.getDatabase(mongoDatabaseName)
                        .getCollection(context.getProperty("mongo.SN.collection"))
                        .find(eq("_id", sn)).iterator();
        return mongoCursor.hasNext() ? mongoCursor.next() : null;
    }

    private Document queryFromSPM(String sn) {
        Document _r = new Document();

        return _r;
    }

    private String mongoDatabaseName;
    private String AIMCollection;
    private String AIMNotJoinedCollection;
    private void storeJoinedData(String id, Document in) {
        in.put("_id", id);
        MongoSingle.getInstance().getLock().writeLock().lock();
        try {
//            log.info("---- " + in.toJson());
            MongoSingle.getInstance().getMongoClient()
                    .getDatabase(mongoDatabaseName).getCollection(AIMCollection)
                    .replaceOne(eq("_id", id), in, new UpdateOptions().upsert(true));
        } catch (Exception e) {

        }
        MongoSingle.getInstance().getLock().writeLock().unlock();
//        mongo.getDatabase(mongoDatabaseName).getCollection(AIMCollection)
//                .replaceOne(eq("_id", id), in, new UpdateOptions().upsert(true));
    }
    private void storeNotJoinedData(String id, Document in) {
        in.put("_id", id);
//        log.info("store unjoined data");
        MongoSingle.getInstance().getLock().writeLock().lock();
        try {
            MongoSingle.getInstance().getMongoClient()
                    .getDatabase(mongoDatabaseName).getCollection(AIMNotJoinedCollection)
                    .replaceOne(eq("_id", id), in, new UpdateOptions().upsert(true));
        } catch (Exception e) {

        }
        MongoSingle.getInstance().getLock().writeLock().unlock();
//        mongo.getDatabase(mongoDatabaseName).getCollection(AIMNotJoinedCollection)
//                .replaceOne(eq("_id", id), in, new UpdateOptions().upsert(true));
    }

}
