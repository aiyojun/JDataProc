package com.jpro;

import com.mongodb.MongoClient;
import lombok.extern.log4j.Log4j2;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

@Log4j2
class MooPoo {
    private Queue<MongoClient> clients;

    MooPoo(String ip, int port, int size) {
        log.info("Create Mongo connection pool, ip - " + ip + "; port - " + port + "; pool size - " + size);
        clients = new ConcurrentLinkedDeque<>();

        for (int i = 0; i < size; i++) {
            MongoClient client = new MongoClient(ip, port);
            clients.add(client);
        }
    }

    MongoClient getMongoClient() {
        if (clients.size() == 0) {
            log.error("Warning! No mongo client in MooPoo! Please check!");
        }
        return clients.remove();
    }

    void returnMongoClient(MongoClient cli) {
        if (cli == null)
            throw new RuntimeException("return Mongo Client error - empty object");
        clients.add(cli);
    }
}
