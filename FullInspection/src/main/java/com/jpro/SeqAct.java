package com.jpro;

import com.alibaba.fastjson.JSONObject;

public interface SeqAct {
    static class FilterState {
        public static final boolean PASS = true;
        public static final boolean NOT_PASS = true;
    }
    public void trap(String s);
    public boolean validate(JSONObject jo);
    public boolean filter(JSONObject jo);
    public void process(JSONObject jo);
}
