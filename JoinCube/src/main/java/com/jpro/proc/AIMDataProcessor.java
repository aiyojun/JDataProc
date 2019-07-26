package com.jpro.proc;

import com.jpro.base.*;
import com.mongodb.MongoClient;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.mongodb.client.model.Filters.*;

@Log4j2
public class AIMDataProcessor implements AbstractDataProcessor {

    private MongoClient mongo;

    public AIMDataProcessor(Properties p) {
        try {
            mongo = new MongoClient(p.getProperty("mongo.server.ip"),
                    Integer.parseInt(p.getProperty("mongo.server.port")));
        } catch (Exception e) {
            log.error("Mongo link failed - " + e);
        }
        AIMExceptionCollection  = p.getProperty("mongo.AIM.exception.collection");

        /// TODO: inner varibles preparations
        timeIncrement       = JComToo.T().parseTimeDiff(p.getProperty("AIM.time.diff").toLowerCase()) * 60 * 60 * 1000;
        timestampKey        = p.getProperty("AIM.timestamp.key");
        passKey             = p.getProperty("AIM.pass.key");
        dataKey             = p.getProperty("AIM.data.key");
        resultsKey          = p.getProperty("AIM.results.key");
        measurementsKey     = p.getProperty("AIM.measurements.key");
        stationKey          = p.getProperty("AIM.station.key");
        processKey          = p.getProperty("AIM.process.key");
        processValueToCNC   = p.getProperty("AIM.process.value1");
        processValueToSPM   = p.getProperty("AIM.process.value2");

        time1Key            = p.getProperty("AIM.time1.key");
        time2Key            = p.getProperty("AIM.time2.key");
        time3Key            = p.getProperty("AIM.time3.key");
        spcValueKey         = p.getProperty("AIM.spc.value.key");
        spcUpperLimitKey    = p.getProperty("AIM.spc.upperlimit.key");
        spcLowerLimitKey    = p.getProperty("AIM.spc.lowerlimit.key");
        spcPassKey          = p.getProperty("AIM.spc.pass.key");
    }

    @Override
    public Document parse(String in) {
        return Document.parse(in);
    }

    private String AIMExceptionCollection;
    @Override
    public void trap(String in) {
        Document row = new Document("raw", in);
        mongo.getDatabase(GlobalContext.mongoDatabase).getCollection(AIMExceptionCollection).insertOne(row);
    }

    private String timestampKey;
    private String passKey;
    private String dataKey;
    private String resultsKey;
    private String stationKey;
    private String processKey;
    private String measurementsKey;
    @Override
    public void validate(Document in) {
        long now = System.currentTimeMillis();
        if (JComToo.statisticOfLastTime == 0) {
            JComToo.statisticLock.writeLock().lock();
            JComToo.statisticOfLastTime = now;
            JComToo.statisticLock.writeLock().unlock();
        } else {
            long delta = now - JComToo.statisticOfLastTime;
            if (delta > 10 * 1000) {
                JComToo.statisticLock.writeLock().lock();
                log.info("statistic query  : " + JComToo.statisticOfQuery.longValue() + "\t / " + (delta/1000) + "\t s\t avg : "  + JComToo.statisticOfQuery.longValue()*1000 / delta);
                log.info("statistic insert : " + JComToo.statisticOfInsert.longValue() + "\t / " + (delta/1000)+ "\t s\t avg : " + JComToo.statisticOfInsert.longValue()*1000 / delta);
                log.info("statistic delete : " + JComToo.statisticOfDelete.longValue() + "\t / " + (delta/1000) + "\t s\t avg : " + JComToo.statisticOfDelete.longValue()*1000 / delta);
                JComToo.statisticOfInsert.reset();
                JComToo.statisticOfQuery.reset();
                JComToo.statisticOfDelete.reset();
                JComToo.statisticOfLastTime = now;
                JComToo.statisticLock.writeLock().unlock();
            }
        }
        // TODO: validate minimum requirement
        if (in.containsKey(GlobalContext.uniqueIdKeyOfAim)
                && in.get(GlobalContext.uniqueIdKeyOfAim) instanceof String
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
        Document _r = new Document(GlobalContext.uniqueIdKeyOfAim, in.getString(GlobalContext.uniqueIdKeyOfAim))
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
            if (k.equals(GlobalContext.uniqueIdKeyOfAim) || k.equals(timestampKey) || k.equals(passKey)
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
        String uniqueIDValue = in.getString(GlobalContext.uniqueIdKeyOfAim);
        String stationValue = in.getString(stationKey);
        String processValue = in.getString(processKey);
        if (processValue.toLowerCase().equals(processValueToCNC.toLowerCase())) {
            Document SNNData = MongoProxy.queryFromSn(uniqueIDValue, mongo);
            Document CNCData = CNCDataProcessor.query(uniqueIDValue, mongo);
            if ((SNNData != null && !SNNData.isEmpty()) && (CNCData != null && !CNCData.isEmpty())) {
                JComToo.gatherDocument(in, SNNData);
                JComToo.gatherDocument(in, CNCData);
                MongoProxy.upsertToAim(uniqueIDValue, processValue, stationValue, in);
                _r = in;
            } else if ((SNNData == null || SNNData.isEmpty()) && (CNCData == null || CNCData.isEmpty())) {
                _r = new Document("AIM", in);
                MongoProxy.upsertToUnjoined(uniqueIDValue, processValue, stationValue, _r);
            } else if ((SNNData == null || SNNData.isEmpty()) && (CNCData != null && !CNCData.isEmpty())) {
                log.warn("Rare branch, the possibility of unique ID didn't come is small. - " + in.toJson());
                _r = new Document("AIM", in).append("CNC", CNCData);
                MongoProxy.upsertToUnjoined(uniqueIDValue, processValue, stationValue, _r);
            } else if ((SNNData != null && !SNNData.isEmpty()) && (CNCData == null || CNCData.isEmpty())) {
                _r = new Document("AIM", in).append("SN", SNNData);
                MongoProxy.upsertToUnjoined(uniqueIDValue, processValue, stationValue, _r);
            }
        } else if (processValue.toLowerCase().equals(processValueToSPM.toLowerCase())) {
            /// TODO: SPM
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

    }

    private static Document doSecondJoin(Document in) {
        Document row = new Document();
        if (in.containsKey("CNC") && in.containsKey("SN") && in.containsKey("AIM")) {
            JComToo.gatherDocument(row, in.get("AIM", Document.class));
            JComToo.gatherDocument(row, in.get("CNC", Document.class));
            JComToo.gatherDocument(row, in.get("SN" , Document.class));
            return row;
        } else if (in.containsKey("SPM") && in.containsKey("SN") && in.containsKey("AIM")) {
            JComToo.gatherDocument(row, in.get("AIM", Document.class));
            JComToo.gatherDocument(row, in.get("SPM", Document.class));
            JComToo.gatherDocument(row, in.get("SN" , Document.class));
            return row;
        }
        return null;
    }

    public static void processSecondJoin(String uniqueValue, Document unjoined, MongoClient mongoClient) {
        Document secondJoinData = AIMDataProcessor.doSecondJoin(unjoined);
        if (secondJoinData != null) {
            mongoClient.getDatabase(GlobalContext.mongoDatabase).getCollection(GlobalContext.collectionOfUnjoined)
                    .findOneAndDelete(eq("_id", unjoined.getString("_id")));
            JComToo.statisticOfDelete.add(1);

            secondJoinData.put("_id", unjoined.getString("_id"));

            GlobalBloom.getFilterForAIM().put(uniqueValue);

            MongoProxy.upsertToAim(secondJoinData);
        } else {
            MongoProxy.upsertToUnjoined(unjoined);
        }
    }

}
