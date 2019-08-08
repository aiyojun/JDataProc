package com.jpro;

import com.alibaba.fastjson.JSONObject;

public class JsonUtil {
    public static String gs(JSONObject j, String k) {
        return j.getString(k);
    }
}
