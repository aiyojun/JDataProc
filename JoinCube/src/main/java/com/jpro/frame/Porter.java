package com.jpro.frame;

import com.jpro.base.BaseBlock;
import com.jpro.base.GlobalContext;
import com.jpro.proc.AIMDataProcessor;
import com.jpro.proc.AbstractDataProcessor;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.concurrent.ArrayBlockingQueue;

@Log4j2
public class Porter {

    private String innerId = "Default";

    private ArrayBlockingQueue<BaseBlock> sharedQueue;

//    private boolean isWorking = false;

    private AbstractDataProcessor processor;

    public Porter(ArrayBlockingQueue<BaseBlock> queue) {
        sharedQueue = queue;
        processor = new AIMDataProcessor(GlobalContext.context);
    }

    public Porter(AbstractDataProcessor proc, ArrayBlockingQueue<BaseBlock> queue) {
        sharedQueue = queue;
        processor = proc;
    }

    public void setInnerId(String id) {
        innerId = id;
    }

//    public void close() {
//        isWorking = false;
//    }

    public void work() {
//        isWorking = true;
        while (true) {
            // TODO: obtain data to consume
            BaseBlock dataToJoin = null;
            try {
//                log.error("Push BaseBlock - " + e);
                if (!GlobalContext.isWorking.get()
                        && sharedQueue.size() == 0) {
                    break;
                }
                dataToJoin = sharedQueue.take();
            } catch (InterruptedException e) {
                log.error(innerId + " Take data from SharedQueue - " + e);
            }
            if (dataToJoin == null) continue;

            if (dataToJoin.getHead().equals("exit")) {
//                log.info(innerId + " Porter received [ exit ] command, exiting ...");
                break;
            }

            // TODO: parse json
            Document origin = null;
            try {
                origin = processor.parse(dataToJoin.getBoby());
            } catch (Exception e) {
                log.error("Parse json - " + e);
                processor.trap(dataToJoin.getBoby());
                continue;
            }
            if (origin == null) continue;

            // TODO: validate original data(fields)
            try {
                processor.validate(origin);
            } catch (Exception e) {
                log.error("Validate process - " + e);
                processor.trap(dataToJoin.getBoby());
                continue;
            }

            // TODO: filter unnecessary data
            Document rowOfFilter = null;
            try {
                rowOfFilter = processor.filter(origin);
            } catch (Exception e) {
                log.error("Filter process - " + e);
                processor.trap(dataToJoin.getBoby());
                continue;
            }
            if (rowOfFilter == null || rowOfFilter.isEmpty()) continue;

            // TODO: process data
            Document rowOfTransform = null;
            try {
                rowOfTransform = processor.transform(rowOfFilter);
            } catch (Exception e) {
                log.error("Transform process - " + e);
                processor.trap(dataToJoin.getBoby());
                continue;
            }
            if (rowOfTransform == null) continue;

            // TODO: core process
            Document rowOfFinal = null;
            try {
                rowOfFinal = processor.core(rowOfTransform);
            } catch (Exception e) {
                log.error("Core process - " + e);
            }
            if (rowOfFinal == null) continue;

            // TODO: final process
            processor.post(rowOfFinal);
        }
        log.info(innerId + " Porter exit loop!");
    }
}
