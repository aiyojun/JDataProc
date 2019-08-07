package com.jpro;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import lombok.extern.log4j.Log4j2;

import static com.jpro.TravelActor.Utils.gs;
import static com.jpro.Unique.*;
import static com.jpro.Unique.Utils.gtv;

@Log4j2
public class TravelActor implements SeqAct {
    private MongoClient qMon = new MongoClient(mongoUrl);
    private MongoDatabase qDisk = qMon.getDatabase(mongoDB);

    @Override
    public void trap(String s) {
        log.info("trapping ... " + s);
    }

    static private String ikUniqueID     = gtv(elfin, "travel.unique.id.key");
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
        log.info("filter ok!");
        return true;
    }

    static private String prefixTab = gtv(elfin, "travel.station.table.prefix");
    @Override
    public void process(JSONObject jo) {
        String vCompose = gs(jo, ikCompose);
        String vUnique  = gs(jo, ikUniqueID);
        String vExtraID = gs(jo, ikExtraID);
        String vStationID = gs(jo, ikStationID);
//        String joinTab  = prefixTab +
        log.info("processing ....");

    }

    static class Utils {
        public static String gs(JSONObject j, String k) {
            return j.getString(k);
        }
    }
}
