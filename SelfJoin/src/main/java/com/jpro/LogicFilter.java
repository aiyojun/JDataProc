package com.jpro;

import java.util.List;
import java.util.Map;

public class LogicFilter implements AbstraFilter {
    private String filterField;
    List<String> passValues;

    LogicFilter(String filterField, List<String> passValues) {
        this.filterField = filterField;
        this.passValues = passValues;
    }

    @Override
    public Map<String, Object> filter(Map<String, Object> kv) {

        return null;
    }

    @Override
    public boolean keepOrNot(Map<String, Object> kv) {

        return false;
    }
}