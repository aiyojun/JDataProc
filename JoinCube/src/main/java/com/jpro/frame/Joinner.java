package com.jpro.frame;

import com.jpro.base.BaseBlock;
import com.jpro.proc.AIMDataProcessor;
import com.jpro.proc.AbstractDataProcessor;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import static com.jpro.base.JComToo.*;

@Log4j2
public class Joinner {

    private Properties context;

    private ArrayBlockingQueue<BaseBlock> sharedQueue;

    private boolean isWorking = false;

    private AbstractDataProcessor processor;

    public Joinner(Properties p, ArrayBlockingQueue<BaseBlock> queue) {
        context = p;
        sharedQueue = queue;
        processor = new AIMDataProcessor(p);
    }

    public void close() {
        isWorking = false;
    }

    public void work() {
        isWorking = true;
        while (isWorking) {
            // TODO: obtain data to consume
            BaseBlock dataToJoin = null;
            try {
                dataToJoin = sharedQueue.take();
            } catch (InterruptedException e) {
                log.error("Take data from SharedQueue - " + e);
            }
            if (dataToJoin == null) continue;

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
            Document rowOfFinal = processor.core(rowOfTransform);

            // TODO: final process
            processor.post(rowOfFinal);
        }
    }
}
