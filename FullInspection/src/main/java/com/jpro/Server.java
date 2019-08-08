package com.jpro;

import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static com.jpro.Unique.Utils.gtv;
import static com.jpro.Unique.Utils.gtv2int;
import static com.jpro.Unique.elfin;

@Log4j2
public class Server {

    public static Server HDL = null;

    List<Dispatcher> dispatchers = new ArrayList<>();

    ArrayBlockingQueue<Msg> sharedQueue0 = new ArrayBlockingQueue<>(gtv2int(elfin, "share.buffer.size"));
    ArrayBlockingQueue<Msg> sharedQueue1 = new ArrayBlockingQueue<>(gtv2int(elfin, "share.buffer.size"));
    ArrayBlockingQueue<Msg> sharedQueue2 = new ArrayBlockingQueue<>(gtv2int(elfin, "share.buffer.size"));

    Provider provider0;
    Provider provider1;
    Provider provider2;

    public Server prepare() {
        if (HDL != null) {
            log.error("Only one Server can be created!");
            System.exit(2);
        }
        HDL = this;
        specify(gtv2int(elfin, "thread.size.travel"), "Travel", sharedQueue0);
        specify(gtv2int(elfin, "thread.size.defect"), "Defect", sharedQueue1);
        specify(gtv2int(elfin, "thread.size.cnc"), "CNC", sharedQueue2);
        provider0 = new Provider(sharedQueue0, "kafka-travel")
                .prepare(gtv(elfin, "kafka.travel.topic"), gtv(elfin, "kafka.travel.group.id"));
        provider1 = new Provider(sharedQueue1, "kafka-defect")
                .prepare(gtv(elfin, "kafka.defect.topic"), gtv(elfin, "kafka.defect.group.id"));
        provider2 = new Provider(sharedQueue2, "kafka-cnc")
                .prepare(gtv(elfin, "kafka.cnc.topic"), gtv(elfin, "kafka.cnc.group.id"));
        return this;
    }

    private void specify(int size, String id, ArrayBlockingQueue<Msg> sharedQueue) {
        for (int i = 0; i < size; i++) {
            Dispatcher ptr = new Dispatcher(new TravelActor(), sharedQueue, id + "-" + i);
            dispatchers.add(ptr);
        }
    }

    public void start() {
        for (int i = 0; i < dispatchers.size(); i++) {
            dispatchers.get(i).start();
        }
        provider0.start();
        provider1.start();
        provider2.run();
    }

    public void close() {
        for (int i = 0; i < gtv2int(elfin, "thread.size.travel") * 2; i++) {
            try {
                sharedQueue0.put(new Msg("exit", ""));
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
        for (int i = 0; i < gtv2int(elfin, "thread.size.defect") * 2; i++) {
            try {
                sharedQueue1.put(new Msg("exit", ""));
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
        for (int i = 0; i < gtv2int(elfin, "thread.size.cnc") * 2; i++) {
            try {
                sharedQueue2.put(new Msg("exit", ""));
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
    }
}
