package com.jpro;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;

import static com.jpro.JsonUtil.gs;
import static com.jpro.Unique.*;
import static com.jpro.Unique.Utils.gtv;
import static com.mongodb.client.model.Filters.eq;

public class DefectActor implements SeqAct {

    private MongoClient qMon = new MongoClient(mongoUrl.ip, mongoUrl.port);
    private MongoDatabase qDisk = qMon.getDatabase(mongoDB);

    static public String ikUniqueID   = gtv(elfin, "defect.unique.id.key");
    static public String ikFailID     = gtv(elfin, "defect.fail.id.key");
    static public String ikDefectID   = gtv(elfin, "defect.id.key");
    static public String ikStationID  = gtv(elfin, "defect.station.id.key");

    @Override
    public void trap(String s) {

    }

    @Override
    public boolean validate(JSONObject jo) {
        return jo.get(ikUniqueID) instanceof String
                && jo.get(ikFailID) instanceof String
                && jo.get(ikDefectID) instanceof String
                && jo.get(ikStationID) instanceof String;
    }

    @Override
    public boolean filter(JSONObject jo) {
        return FilterState.PASS;
    }

    private static String ysDefectID         = gtv(elfin, "sys.defect.id.key");
    private static String ysDefectDescCn     = gtv(elfin, "sys.defect.desc.cn.key");
    private static String ysDefectDescEn     = gtv(elfin, "sys.defect.desc.en.key");
    private static String ysDefectType       = gtv(elfin, "sys.defect.type.key");

    private static String okDefectID   = gtv(elfin, "defect.gen.id.key");
    private static String okDefectEn   = gtv(elfin, "defect.gen.desc.en.key");
    private static String okDefectCn   = gtv(elfin, "defect.gen.desc.cn.key");
    private static String okDefectType = gtv(elfin, "defect.gen.type.key");
    static private String prefixTab = gtv(elfin, "travel.station.table.prefix");
    @Override
    public void process(JSONObject jo) {
        String vStationID = gs(jo, ikStationID);
        String vRECID = gs(jo, ikFailID);
        String vDefectID = gs(jo, ikDefectID);
        String vUnique = gs(jo, ikUniqueID);

        MongoCursor<Document> cursor = qDisk.getCollection(defectSysTab).find(eq(ysDefectID, vDefectID)).iterator();
        Document defectAttrDoc = cursor.hasNext() ? cursor.next() : null;
        cursor.close();
        if (defectAttrDoc == null)
            throw new RuntimeException("cannot find the defect type in " + defectSysTab);

        JSONObject defectAttr = JSON.parseObject(defectAttrDoc.toJson());
        if (defectAttr.get(ysDefectID) instanceof String
                && defectAttr.get(ysDefectType) instanceof String
                && defectAttr.get(ysDefectDescEn) instanceof String
                && defectAttr.get(ysDefectDescCn) instanceof String) {

        } else {
            throw new RuntimeException("defect dictionary table field type error - " + defectSysTab);
        }

        JSONObject fly = new JSONObject();
        fly.put(okDefectID, defectAttr.getString(ysDefectID));
        fly.put(okDefectEn, defectAttr.getString(ysDefectDescEn));
        fly.put(okDefectCn, defectAttr.getString(ysDefectDescCn));
        fly.put(okDefectType, defectAttr.getString(ysDefectType));

        // TODO: store anyhow
        qDisk.getCollection(defectTab).insertOne(Document.parse(fly.toJSONString()));

        String joinTab = prefixTab + stationMapper.get(vStationID).getName().toLowerCase().replace('-', '_');

        cursor = qDisk.getCollection(joinTab).find(eq(TravelActor.ikUniqueID, vUnique)).iterator();
        Document joinedDoc = cursor.hasNext() ? cursor.next() : null;

        if (joinedDoc == null) return;
        joinedDoc.remove("_id");

        // TODO: update travel data
        JSONObject joined = JSON.parseObject(joinedDoc.toJson());
        if (joined.containsKey(TravelActor.okGenDefectKey)) {
            JSONArray defectArr = joined.getJSONArray(TravelActor.okGenDefectKey);
            boolean has = false;
            for (int i = 0; i < defectArr.size(); i++) {
                if (defectArr.getJSONObject(i).getString(okDefectID).equals(fly.getString(okDefectID))) {
                    has = true;
                }
            }
            if (!has) {
                defectArr.add(fly);
                qDisk.getCollection(joinTab).replaceOne(
                        eq(TravelActor.ikUniqueID, vUnique),
                        Document.parse(joined.toJSONString()),
                        new ReplaceOptions().upsert(true));
            }
        } else {
            JSONArray defectArr = new JSONArray();
            defectArr.add(fly);
            joined.put(TravelActor.okGenDefectKey, defectArr);
            qDisk.getCollection(joinTab).replaceOne(
                    eq(TravelActor.ikUniqueID, vUnique),
                    Document.parse(joined.toJSONString()),
                    new ReplaceOptions().upsert(true));
        }
    }
}
