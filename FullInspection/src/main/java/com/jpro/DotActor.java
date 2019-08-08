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

public class DotActor implements SeqAct {

    private MongoClient qMon = new MongoClient(mongoUrl.ip, mongoUrl.port);
    private MongoDatabase qDisk = qMon.getDatabase(mongoDB);

    public static String ikUniqueID     = gtv(elfin, "cnc.unique.id.key");
    public static String ikStationName  = gtv(elfin, "cnc.station.name.key");
    public static String ikMachineName  = gtv(elfin, "cnc.machine.name.key");
    public static String ikCell         = gtv(elfin, "cnc.cell.key");

    @Override
    public void trap(String s) {

    }

    @Override
    public boolean validate(JSONObject jo) {
        return jo.get(ikUniqueID) instanceof String
                && jo.get(ikStationName) instanceof String
                && jo.get(ikMachineName) instanceof String
                && jo.get(ikCell) instanceof String;
    }

    @Override
    public boolean filter(JSONObject jo) {
        return FilterState.PASS;
    }

    @Override
    public void process(JSONObject jo) {
        // TODO: store first
        qDisk.getCollection(dotTab).insertOne(Document.parse(jo.toJSONString()));

        String vStationName = gs(jo, ikStationName);
        String vUnique = gs(jo, ikUniqueID);
        String vMachineName = gs(jo, ikMachineName);
        String vCell = gs(jo, ikCell);

        JSONObject fly = new JSONObject();
        fly.put(ikStationName, vStationName);
        fly.put(ikMachineName, vMachineName);
        fly.put(ikCell, vCell);

        String lcStationName = vStationName.toLowerCase();
        if (lcStationName.contains("cnc7") || lcStationName.contains("cnc8")) {
            updateTravelOperation(
                    vUnique,
                    TravelActor.prefixTab + stationMapper.get("105399").getName().toLowerCase().replace('-', '_'),
                    fly);
            updateTravelOperation(
                    vUnique,
                    TravelActor.prefixTab + stationMapper.get("600000100").getName().toLowerCase().replace('-', '_'),
                    fly);
            updateTravelOperation(
                    vUnique,
                    TravelActor.prefixTab + stationMapper.get("105580").getName().toLowerCase().replace('-', '_'),
                    fly);
        } else if (lcStationName.contains("cnc9") || lcStationName.contains("cnc10")) {
            updateTravelOperation(
                    vUnique,
                    TravelActor.prefixTab + stationMapper.get("600000100").getName().toLowerCase().replace('-', '_'),
                    fly);
            updateTravelOperation(
                    vUnique,
                    TravelActor.prefixTab + stationMapper.get("105580").getName().toLowerCase().replace('-', '_'),
                    fly);
        }
    }

    private void updateTravelOperation(String vUnique, String joinTab, JSONObject dotFly) {
        MongoCursor<Document> cursor = qDisk.getCollection(joinTab).find(eq(TravelActor.ikUniqueID, vUnique)).iterator();
        Document joinedDoc = cursor.hasNext() ? cursor.next() : null;
        if (joinedDoc == null) return;

        JSONObject joined = JSON.parseObject(joinedDoc.toJson());

        if (joined.get("CNC") instanceof JSONArray) {
            JSONArray dotArr = joined.getJSONArray("CNC");
            boolean hasSame = false;
            for (int i = 0; i < dotArr.size(); i++) {
                if (dotArr.getJSONObject(i).getString(ikStationName).equals(dotFly.getString(ikStationName))) {
                    hasSame = true;
                    break;
                }
            }
            if (!hasSame) {
                dotArr.add(dotFly);
                joined.put("CNC", dotArr);
                qDisk.getCollection(joinTab).replaceOne(
                        eq(TravelActor.ikUniqueID, vUnique),
                        Document.parse(joined.toJSONString()),
                        new ReplaceOptions().upsert(true));
            }
        } else {
            JSONArray dotArr = new JSONArray();
            dotArr.add(dotFly);
            joined.put("CNC", dotArr);
            qDisk.getCollection(joinTab).replaceOne(
                    eq(TravelActor.ikUniqueID, vUnique),
                    Document.parse(joined.toJSONString()),
                    new ReplaceOptions().upsert(true));
        }
    }
}
