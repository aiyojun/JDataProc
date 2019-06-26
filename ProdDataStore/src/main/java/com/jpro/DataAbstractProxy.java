package com.jpro;

public interface DataAbstractProxy {
    void link(String ip, int port);

    void insert(String data);

    void close();
}
