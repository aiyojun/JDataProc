package com.jpro;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil {
    public static long TimeString2Long(String s) {
        Date t = null;
        try {
            t = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(s);
        } catch (ParseException e) {
            throw new RuntimeException("parse timestamp error - " + e);
        }
        return t.getTime();
    }
}
