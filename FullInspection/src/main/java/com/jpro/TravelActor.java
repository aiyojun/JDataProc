package com.jpro;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jpro.JsonUtil.gs;
import static com.jpro.Unique.*;
import static com.jpro.Unique.Utils.gtv;
import static com.mongodb.client.model.Filters.eq;

@Log4j2
public class TravelActor implements SeqAct {
    private MongoClient qMon = new MongoClient(mongoUrl.ip, mongoUrl.port);
    private MongoDatabase qDisk = qMon.getDatabase(mongoDB);

    @Override
    public void trap(String s) {
        log.info("trapping ... " + s);
    }

    static public String ikUniqueID     = gtv(elfin, "travel.unique.id.key");
    static private String ikCompose      = gtv(elfin, "travel.compose.key");
    static private String ikExtraID      = gtv(elfin, "travel.extra.id.key");
    static private String ikStationID    = gtv(elfin, "travel.station.id.key");
    static private String ikStateOne     = gtv(elfin, "travel.state.one.key");
    static private String ikStateTwo     = gtv(elfin, "travel.state.two.key");
    static private String ikOutTime      = gtv(elfin, "travel.out.time.key");
    static private String ikFailID       = gtv(elfin, "travel.fail.id.key");
    @Override
    public boolean validate(JSONObject jo) {
        log.info("validate success, passing ...");
        return jo.get(ikUniqueID) instanceof String
                && jo.get(ikCompose) instanceof String
                && jo.get(ikExtraID) instanceof String
                && jo.get(ikStationID) instanceof String
                && jo.get(ikStateOne) instanceof String
                && jo.get(ikStateTwo) instanceof String
                && jo.get(ikOutTime) instanceof String
                && jo.get(ikFailID) instanceof String;
    }

    @Override
    public boolean filter(JSONObject jo) {
//        log.info("filter ok!");
        return FilterState.PASS;
    }

    static public String prefixTab = gtv(elfin, "travel.station.table.prefix");
    static public String okGenDefectKey = gtv(elfin, "travel.gen.json.defect.key");
    static private String okGenState     = gtv(elfin, "travel.gen.state.key");
    static private String okGenrstTime   = gtv(elfin, "travel.gen.rst.time.key");
    static private String okGenCompose   = gtv(elfin, "travel.gen.compose.key");
    static private String okGenAuto      = gtv(elfin, "travel.gen.auto.key");
    static private String ovGenAuto      = gtv(elfin, "travel.gen.auto.val");
    static private String okGenAuto2     = gtv(elfin, "travel.gen.auto2.key");
    static private String ovGenAuto2     = gtv(elfin, "travel.gen.auto2.val");
    static private String okGenStation   = gtv(elfin, "travel.gen.station.name.key");
    static private String okGenColor     = gtv(elfin, "travel.gen.color.key");
    static private String okGenMedia     = gtv(elfin, "travel.gen.media.key");
    static private String okGenProduct   = gtv(elfin, "travel.gen.product.key");
    static private String okGenProductID = gtv(elfin, "travel.gen.product.id.key");
    static public final String StatePass = "pass";
    static public final String StateFail = "fail";
    static public final String StateScrapped = "scrapped";
    static public final String StateRework = "rework";
    static public final String StateFailRework = "fail_rework";
    @Override
    public void process(JSONObject jo) {
        String vUnique  = gs(jo, ikUniqueID);
        String vStateOne = gs(jo, ikStateOne);
        String vStateTwo = gs(jo, ikStateTwo);
        String vStationID = gs(jo, ikStationID);

        // TODO: branch [ rework data | not rework data ]
        if (!reworkMapper.containsKey(vStationID)) {
            String vStationName = stationMapper.get(reworkMapper.get(vStationID)).getName();
            String joinTab  = prefixTab + vStationName.toLowerCase().replace('-', '_');

            // TODO: query history data
            MongoCursor<Document> cursor = qDisk.getCollection(joinTab).find(eq(ikUniqueID, vUnique)).iterator();
            Document historyDoc = (cursor.hasNext() ? cursor.next() : null);

            if (historyDoc == null) return;

            // TODO: update result state
            JSONObject historyJson = JSON.parseObject(historyDoc.toJson());
            historyDoc.remove("_id");
            if (vStateOne.equals("0") && vStateTwo.equals("0")) {
                historyJson.put(okGenState, StateRework);
            } else if (vStateOne.equals("1") && vStateTwo.equals("0")) {
                historyJson.put(okGenState, StateFailRework);
            } else if (vStateOne.equals("1") && vStateTwo.equals("1")) {
                historyJson.put(okGenState, StateScrapped);
            } else {
                return;
            }
            // TODO: update rework data
            qDisk.getCollection(joinTab).replaceOne(
                    eq(ikUniqueID, vUnique),
                    Document.parse(historyJson.toJSONString()),
                    new ReplaceOptions().upsert(true));
            return;
        } else {
            String vCompose = gs(jo, ikCompose);
            String vExtraID = gs(jo, ikExtraID);
            String vRECID = gs(jo, ikFailID);
            String vStationName = stationMapper.get(vStationID).getName();
            String joinTab  = prefixTab + vStationName.toLowerCase().replace('-', '_');

            // TODO: query history data
            MongoCursor<Document> cursor = qDisk.getCollection(joinTab).find(eq(ikUniqueID, vUnique)).iterator();
            Document historyDoc = (cursor.hasNext() ? cursor.next() : null);
            cursor.close();

            JSONObject fly = null;
            if (historyDoc == null) {
                fly = new JSONObject();

                // TODO: extract fields
                long tim = TimeUtil.TimeString2Long(gs(jo, ikOutTime));
                fly.put(ikUniqueID, vUnique);
                fly.put(okGenrstTime, tim);
                fly.put(ikOutTime, tim);
                fly.put(okGenCompose, makeBuild(vCompose));
                fly.put(okGenAuto, ovGenAuto);
                fly.put(okGenAuto2, "");
                fly.put(okGenState, makeResult(vStateOne, vStateTwo));

                // TODO: generate necessary fields
                fly.put(okGenStation, stationMapper.get(vStationID).getName());
                fly.put(okGenColor, partMapper.get(vExtraID).upccode);
                fly.put(okGenMedia, partMapper.get(vExtraID).jancode);
                fly.put(okGenProduct, partMapper.get(vExtraID).cartonVolume);
                fly.put(okGenProductID, partMapper.get(vExtraID).meterialType);

                switch (vStationID) {
                    case "105399":
                    case "105580":
                    case "600000100":
                        cursor = qDisk.getCollection(dotTab)
                                .find(eq(DotActor.ikUniqueID, vUnique)).iterator();
                        JSONArray dotArr = createDot(MongoUtil.extract(cursor));
                        if (!dotArr.isEmpty()) {
                            fly.put("CNC", dotArr);
                        }
                        break;
                    default:;
                }

                // TODO: query defect data(have or not)
                generateDefectData(fly, vRECID, jo);

                fly.put("mapping", fly.getString(okGenAuto)
                        + fly.getString(okGenProduct)
                        + fly.getString(okGenCompose)
                        + fly.getString(okGenColor)
                        + fly.getString(okGenMedia));
            } else {
                fly = JSON.parseObject(historyDoc.toJson());
                fly.put(ikOutTime, gs(jo, ikOutTime));
                fly.put(okGenState, makeResult(vStateOne, vStateTwo));

                // TODO: query defect data(have or not)
                generateDefectData(fly, vRECID, jo);
            }

            qDisk.getCollection(joinTab).replaceOne(
                    eq(ikUniqueID, vUnique),
                    Document.parse(fly.toJSONString()),
                    new ReplaceOptions().upsert(true));
        }
    }

    private void generateDefectData(JSONObject fly, String recID, JSONObject jo) {
        // TODO: query defect data(have or not)
        MongoCursor<Document> cursor = qDisk.getCollection(defectTab).find(eq(DefectActor.ikFailID, recID)).iterator();
        Document defectDataDoc = cursor.hasNext() ? cursor.next() : null;
        if (defectDataDoc != null) {
            defectDataDoc.remove("_id");
            if (!jo.containsKey(okGenDefectKey)) {
                JSONArray defectArr = new JSONArray();
                defectArr.add(JSON.parseObject(defectDataDoc.toJson()));
                fly.put(okGenDefectKey, defectArr);
            } else {
                JSONArray defectArr = jo.getJSONArray(okGenDefectKey);
                defectArr.add(JSON.parseObject(defectDataDoc.toJson()));
                fly.put(okGenDefectKey, defectArr);
            }
        }
        cursor.close();
    }

    private JSONArray createDot(List<Document> li) {
        JSONArray _r = new JSONArray();
        Map<String, Document> core = new HashMap<>();
        for (Document ele : li) {
            if (ele.get(DotActor.ikStationName) instanceof String) {
                String vStationName = ele.getString(DotActor.ikStationName);
                if (core.containsKey(vStationName) && ele.getObjectId("_id").getTimestamp()
                        <= core.get(vStationName).getObjectId("_id").getTimestamp()) {
                    continue;
                }
                core.put(vStationName, ele);
            }
        }
        for (Map.Entry<String, Document> ele : core.entrySet()) {
            JSONObject one = new JSONObject();
            one.put(DotActor.ikCell, ele.getValue().getString(DotActor.ikCell));
            one.put(DotActor.ikMachineName, ele.getValue().getString(DotActor.ikMachineName));
            one.put(DotActor.ikStationName, ele.getValue().getString(DotActor.ikStationName));
            _r.add(one);
        }
        return _r;
    }

    private String makeResult(String p1, String p2) {
        if (p1.equals("0") && p2.equals("0")) {
            return StatePass;
        } else if (p1.equals("1") && p2.equals("0")) {
            return StateFail;
        } else if (p1.equals("1") && p2.equals("1")) {
            return StateScrapped;
        } else {
            throw new RuntimeException("unknown result state!");
        }
    }

    private String makeBuild(String s) {
        for (Map.Entry<String, Character> p : buildMapper.entrySet()) {
            if (s.contains(p.getKey()))
                return p.getKey();
        }
        throw new RuntimeException("Cannot find build info in - " + s);
    }

}
