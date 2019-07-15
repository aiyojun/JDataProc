package com.jpro.frame;

import com.jpro.base.JComToo;
import com.jpro.proc.CNCDataProcessor;
import com.jpro.proc.SNDataProcessor;
import com.jpro.proc.SPMDataProcessor;
import lombok.extern.log4j.Log4j2;

import java.util.Properties;

@Log4j2
public class Storage {

    private Properties context;

    private Porter CNCPorter;
    private Porter SNPorter;
    private Porter SPMPorter;

    private Thread CNCPorterThread;
    private Thread SNPorterThread;
    private Thread SPMPorterThread;

    public Storage(Properties p) {
        context = p;
//        JComToo.log("\033[34;1m$$$$\033[0m Storage   -- ");
    }

    public Storage prepare() {
        CNCDataProcessor p1 = new CNCDataProcessor(context);
         SNDataProcessor p2 = new  SNDataProcessor(context);
        SPMDataProcessor p3 = new SPMDataProcessor(context);
        CNCPorter = new Porter(context, p1, p1.makeKafkaConsumer());
         SNPorter = new Porter(context, p2, p2.makeKafkaConsumer());
        SPMPorter = new Porter(context, p3, p3.makeKafkaConsumer());
        CNCPorterThread = new Thread(CNCPorter::work);
         SNPorterThread = new Thread( SNPorter::work);
        SPMPorterThread = new Thread(SPMPorter::work);
        return this;
    }

    public void close() {
        CNCPorter.stop();
         SNPorter.stop();
//        SPMPorter.stop();
    }

    public void start() {
        CNCPorterThread.start();
         SNPorterThread.start();
//        SPMPorterThread.start();
    }

}
