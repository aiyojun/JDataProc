package com.jpro.frame;

import com.jpro.base.BaseBlock;
import com.jpro.base.JComToo;
import com.jpro.base.KafkaProxy;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

@Log4j2
public class Join {

    private Properties context;

    private ArrayBlockingQueue<BaseBlock> sharedQueue;

    private KafkaProxy kafkaProxy;

    private int workerSize;

    private List<Thread> allThreadHandles;

    private List<Joinner> allJoinnerHandles;

    public Join(Properties p) {
        context = p;

        sharedQueue = new ArrayBlockingQueue<>(
                Integer.parseInt(context.getProperty("join.buffer.size")));

        workerSize = Integer.parseInt(context.getProperty("join.thread.number"));

        kafkaProxy = new KafkaProxy(context, sharedQueue);
        log.info("\033[34;1m$$$$\033[0m Join -- ShareQueue \033[33;1m[ "
                + context.getProperty("join.buffer.size") + " ]\033[0m + Join Worker \033[33;1m[ "
                + context.getProperty("join.thread.number") + " ]\033[0m");
    }

    public Join prepare() {
        kafkaProxy.prepare();
        allThreadHandles = new ArrayList<>();
        allJoinnerHandles = new ArrayList<>();
        for (int i = 0; i < workerSize; i++) {
            Joinner joinner = new Joinner(context, sharedQueue);
            Thread td = new Thread(joinner::work);
            allJoinnerHandles.add(joinner);
            allThreadHandles.add(td);
        }
        return this;
    }

    public void start() {
        for (int i = 0; i < workerSize; i++) {
            log.info("Launch Joinner Thread [ " + i + " ].");
            allThreadHandles.get(i).start();
        }
        log.info("Kafka Proxy start [ Main Thread ]");
        kafkaProxy.start();
    }

    public void close() {
        kafkaProxy.close();
        for (int i = 0; i < workerSize; i++) {
            allJoinnerHandles.get(i).close();
            log.info("Joinner Thread [ " + i + " ] exiting.");
        }
    }

}
