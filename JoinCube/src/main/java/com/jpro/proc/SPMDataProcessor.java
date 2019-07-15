package com.jpro.proc;

import com.jpro.base.KafkaProxy;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.util.Collections;
import java.util.Properties;

public class SPMDataProcessor implements AbstractDataProcessor {

    private Properties context;

    public SPMDataProcessor(Properties p) {
        context = p;
    }


    @Override
    public Document parse(String in) {
        return null;
    }

    @Override
    public void trap(String in) {

    }

    @Override
    public void validate(Document in) {

    }

    @Override
    public Document filter(Document in) {
        return null;
    }

    @Override
    public Document transform(Document in) {
        return null;
    }

    @Override
    public Document core(Document in) {
        return null;
    }

    @Override
    public void post(Document in) {

    }

    public KafkaConsumer<String, String> makeKafkaConsumer() {
        Properties conProps = KafkaProxy.makeBaseAttr();
        conProps.put("bootstrap.servers", context.getProperty("kafka.server.url"));
        conProps.put("group.id", context.getProperty("SPM.group.id"));
        KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(conProps);
        consumer.subscribe(Collections.singleton(context.getProperty("SPM.topic")));
        return consumer;
    }
}
