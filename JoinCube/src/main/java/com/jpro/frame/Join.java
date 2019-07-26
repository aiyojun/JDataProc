package com.jpro.frame;

import com.jpro.base.BaseBlock;
import com.jpro.base.GlobalContext;
import com.jpro.base.JComToo;
import com.jpro.base.KafkaProxy;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

@Log4j2
public class Join {

    private ArrayBlockingQueue<BaseBlock> sharedQueue;

    private KafkaProxy kafkaProxy;

    private int workerSize;

    private List<Thread> allThreadHandles;

    private List<Porter> allPorterHandles;

    public Join() {
        Properties context = GlobalContext.getInstance().getContext();

        sharedQueue = new ArrayBlockingQueue<>(
                Integer.parseInt(context.getProperty("join.buffer.size")));

        workerSize = Integer.parseInt(context.getProperty("join.thread.number"));

        kafkaProxy = new KafkaProxy(context, sharedQueue);
        kafkaProxy.setInnerId("AIM");
        JComToo.sleep(0.2);
        log.info("\033[34;1m$$$$\033[0m Join -- ShareQueue \033[33;1m[ "
                + context.getProperty("join.buffer.size") + " ]\033[0m + Join Worker \033[33;1m[ "
                + context.getProperty("join.thread.number") + " ]\033[0m");
        JComToo.sleep(2);
    }

    public Join prepare() {
        kafkaProxy.prepare();
        allThreadHandles = new ArrayList<>();
        allPorterHandles = new ArrayList<>();
        for (int i = 0; i < workerSize; i++) {
            Porter porter = new Porter(sharedQueue);
            porter.setInnerId("Joinner-" + i);
            Thread td = new Thread(porter::work);
            allPorterHandles.add(porter);
            allThreadHandles.add(td);
        }
        return this;
    }

    public void start() {
        for (int i = 0; i < workerSize; i++) {
            log.info("Launch Porter Thread [ " + i + " ].");
            allThreadHandles.get(i).start();
        }
        log.info("Kafka Proxy start [ Main Thread ]");
        kafkaProxy.start();
    }

    public void close() {
        try {
            for (int i = 0; i < workerSize; i++) {
                sharedQueue.put(new BaseBlock("exit", ""));
            }
        } catch (InterruptedException e) {
            log.error("Close BaseBlock - " + e);
        }
//        kafkaProxy.close();
//        for (int i = 0; i < workerSize; i++) {
//            allPorterHandles.get(i).close();
//            log.info("Porter Thread [ " + i + " ] exiting.");
//        }
    }

}
