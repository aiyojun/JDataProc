package com.jpro;

import org.bson.Document;

public interface AbstDataProc {
    Document doVerify(Document orig);
    Document doFilter(Document orig);
    Document generateNotifyData(Document orig);
    Document generateStorageData(Document orig);
}
