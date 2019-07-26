package com.jpro.frame;

import com.jpro.base.BaseBlock;
import com.jpro.base.GlobalContext;
import com.jpro.base.KafkaProxy;
import com.jpro.proc.CNCDataProcessor;
import com.jpro.proc.SNDataProcessor;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

@Log4j2
public class Storage {

    private static Properties context;
    private ArrayBlockingQueue<BaseBlock> sharedQueueOfCNC;
    private KafkaProxy kafkaProxyOfCNC;
    private ArrayBlockingQueue<BaseBlock> sharedQueueOfSN;
    private KafkaProxy kafkaProxyOfSN;
    public Storage() {
        context = GlobalContext.getInstance().getContext();
        sharedQueueOfCNC = new ArrayBlockingQueue<>(
                Integer.parseInt(context.getProperty("join.buffer.size")));
        kafkaProxyOfCNC = new KafkaProxy(context, sharedQueueOfCNC);
        kafkaProxyOfCNC.setInnerId("CNC");
        sharedQueueOfSN = new ArrayBlockingQueue<>(
                Integer.parseInt(context.getProperty("join.buffer.size")));
        kafkaProxyOfSN = new KafkaProxy(context, sharedQueueOfSN);
        kafkaProxyOfSN.setInnerId("SN");
    }

    private int threadNumberOfCNC = 1;
    private List<CNCDataProcessor> processorHandleOfCNC = new ArrayList<>();
    private List<Porter> porterHandleOfCNC = new ArrayList<>();
    private List<Thread> threadHandleOfCNC = new ArrayList<>();
    private Thread kafkaThreadOfCNC;
    private int threadNumberOfSN = 1;
    private List<SNDataProcessor> processorHandleOfSN = new ArrayList<>();
    private List<Porter> porterHandleOfSN = new ArrayList<>();
    private List<Thread> threadHandleOfSN = new ArrayList<>();
    private Thread kafkaThreadOfSN;
    public Storage prepare() {
        kafkaThreadOfCNC = new Thread(kafkaProxyOfCNC::start);
        kafkaProxyOfCNC.prepare(context.getProperty("CNC.topic"), context.getProperty("CNC.group.id"));

        kafkaThreadOfSN = new Thread(kafkaProxyOfSN::start);
        kafkaProxyOfSN.prepare(context.getProperty("SN.topic"), context.getProperty("SN.group.id"));

        threadNumberOfCNC = Integer.parseInt(context.getProperty("storage.CNC.thread.number"));
        for (int i = 0; i < threadNumberOfCNC; i++) {
            CNCDataProcessor p1 = new CNCDataProcessor(context);
            Porter porter = new Porter(p1, sharedQueueOfCNC);
            porter.setInnerId("StorageCNC-" + i);
            Thread td = new Thread(porter::work);
            processorHandleOfCNC.add(p1);
            porterHandleOfCNC.add(porter);
            threadHandleOfCNC.add(td);
        }

        threadNumberOfSN = Integer.parseInt(context.getProperty("storage.SN.thread.number"));
        for (int i = 0; i < threadNumberOfSN; i++) {
            SNDataProcessor p1 = new SNDataProcessor(context);
            Porter porter = new Porter(p1, sharedQueueOfSN);
            porter.setInnerId("StorageSN-" + i);
            Thread td = new Thread(porter::work);
            processorHandleOfSN.add(p1);
            porterHandleOfSN.add(porter);
            threadHandleOfSN.add(td);
        }
        return this;
    }

    public void close() {
        try {
            for (int i = 0; i < threadNumberOfCNC; i++) {
                sharedQueueOfCNC.put(new BaseBlock("exit", ""));
            }
            for (int i = 0; i < threadNumberOfSN; i++) {
                sharedQueueOfSN.put(new BaseBlock("exit", ""));
            }
        } catch (InterruptedException e) {
            log.error("Close BaseBlock - " + e);
        }
//        kafkaProxyOfCNC.close();
//        kafkaProxyOfSN.close();
//        for (int i = 0; i < threadNumberOfCNC; i++) {
//            porterHandleOfCNC.get(i).close();
//            log.info("Launch CNC Porter Thread [ " + i + " ].");
//        }
//        for (int i = 0; i < threadNumberOfSN; i++) {
//            porterHandleOfSN.get(i).close();
//            log.info("Launch SN Porter Thread [ " + i + " ].");
//        }
    }

    public void start() {
        for (int i = 0; i < threadNumberOfCNC; i++) {
            threadHandleOfCNC.get(i).start();
        }
        for (int i = 0; i < threadNumberOfSN; i++) {
            threadHandleOfSN.get(i).start();
        }
        kafkaThreadOfCNC.start();
        kafkaThreadOfSN.start();
        log.info("Kafka Proxy start [ CNC Thread ]");
        log.info("Kafka Proxy start [ SN Thread ]");
    }

}
