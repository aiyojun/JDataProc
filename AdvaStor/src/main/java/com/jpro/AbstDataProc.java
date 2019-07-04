package com.jpro;

import org.bson.Document;

public interface AbstDataProc {
    Document doFilter(Document orig);
    Document generateNotifyData(Document orig);
    Document generateStorageData(Document orig);
}
