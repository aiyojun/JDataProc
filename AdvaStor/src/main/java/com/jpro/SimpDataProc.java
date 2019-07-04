package com.jpro;

import org.bson.Document;

public class SimpDataProc implements AbstDataProc {
    @Override
    public Document doFilter(Document orig) {
        return orig;
    }

    @Override
    public Document generateNotifyData(Document orig) {
        return orig;
    }

    @Override
    public Document generateStorageData(Document orig) {
        return orig;
    }
}
