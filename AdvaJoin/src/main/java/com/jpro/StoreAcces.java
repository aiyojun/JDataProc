package com.jpro;

import java.util.Properties;

class StoreAcces {
    private MooPoo AIM_MOO;
    private MooPoo CNC_MOO;
    private MooPoo SPM_MOO;
    private MooPoo EXC_MOO;
    private MooPoo STO_MOO;
    private MooPoo SNN_MOO;

    private Properties props;

    public StoreAcces(Properties p) {
        props = p;
    }

    public void init() {
        AIM_MOO = new MooPoo(props.getProperty("mongo.ip"),
                Integer.parseInt(props.getProperty("mongo.port")),
                Integer.parseInt(props.getProperty("mongo.pool.size")));
        CNC_MOO = new MooPoo(props.getProperty("mongo.ip"),
                Integer.parseInt(props.getProperty("mongo.port")),
                Integer.parseInt(props.getProperty("mongo.pool.size")));
        SPM_MOO = new MooPoo(props.getProperty("mongo.ip"),
                Integer.parseInt(props.getProperty("mongo.port")),
                Integer.parseInt(props.getProperty("mongo.pool.size")));
        EXC_MOO = new MooPoo(props.getProperty("mongo.ip"),
                Integer.parseInt(props.getProperty("mongo.port")),
                Integer.parseInt(props.getProperty("mongo.pool.size")));
        STO_MOO = new MooPoo(props.getProperty("mongo.ip"),
                Integer.parseInt(props.getProperty("mongo.port")),
                Integer.parseInt(props.getProperty("mongo.pool.size")));
        SNN_MOO = new MooPoo(props.getProperty("mongo.ip"),
                Integer.parseInt(props.getProperty("mongo.port")),
                Integer.parseInt(props.getProperty("mongo.pool.size")));
    }

    public MooPoo getAIM_MOO() {
        return AIM_MOO;
    }

    public MooPoo getCNC_MOO() {
        return CNC_MOO;
    }

    public MooPoo getSPM_MOO() {
        return SPM_MOO;
    }

    public MooPoo getEXC_MOO() {
        return EXC_MOO;
    }

    public MooPoo getSTO_MOO() {
        return STO_MOO;
    }

    public MooPoo getSNN_MOO() {
        return SNN_MOO;
    }
}
