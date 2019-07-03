package com.jpro;

import java.util.Properties;

class StoreAcces {
    private MooPoo AIM_MOO;

    private MooPoo CNC_MOO;
    private MooPoo SPM_MOO;
    private MooPoo SNN_MOO;

    private MooPoo EXC_MOO;

    StoreAcces(Properties p) {
        AIM_MOO = new MooPoo(p.getProperty("mongo.aim.ip"),
                Integer.parseInt(p.getProperty("mongo.aim.port")),
                Integer.parseInt(p.getProperty("mongo.aim.pool.size")));
        CNC_MOO = new MooPoo(p.getProperty("mongo.cnc.ip"),
                Integer.parseInt(p.getProperty("mongo.cnc.port")),
                Integer.parseInt(p.getProperty("mongo.cnc.pool.size")));
        SPM_MOO = new MooPoo(p.getProperty("mongo.spm.ip"),
                Integer.parseInt(p.getProperty("mongo.spm.port")),
                Integer.parseInt(p.getProperty("mongo.spm.pool.size")));
        EXC_MOO = new MooPoo(p.getProperty("mongo.exc.ip"),
                Integer.parseInt(p.getProperty("mongo.exc.port")),
                Integer.parseInt(p.getProperty("mongo.exc.pool.size")));
        SNN_MOO = new MooPoo(p.getProperty("mongo.sn.ip"),
                Integer.parseInt(p.getProperty("mongo.sn.port")),
                Integer.parseInt(p.getProperty("mongo.sn.pool.size")));
    }

    MooPoo getAIM_MOO() {
        return AIM_MOO;
    }

    MooPoo getCNC_MOO() {
        return CNC_MOO;
    }

    MooPoo getSPM_MOO() {
        return SPM_MOO;
    }

    MooPoo getEXC_MOO() {
        return EXC_MOO;
    }

    MooPoo getSNN_MOO() {
        return SNN_MOO;
    }
}
