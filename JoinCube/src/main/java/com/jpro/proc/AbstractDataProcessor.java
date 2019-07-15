package com.jpro.proc;

import org.bson.Document;

public interface AbstractDataProcessor {
    public Document parse(String in);

    public void trap(String in);

    public void validate(final Document in);

    public Document filter(final Document in);

    public Document transform(Document in);

    public Document core(Document in);

    public void post(Document in);
}
