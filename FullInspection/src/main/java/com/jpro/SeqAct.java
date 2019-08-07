package com.jpro;

import com.alibaba.fastjson.JSONObject;

public interface SeqAct {
    public void trap(String s);
    public boolean validate(JSONObject jo);
    public boolean filter(JSONObject jo);
    public void process(JSONObject jo);
}
