package com.jpro;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.ArrayBlockingQueue;

@Log4j2
public class Dispatcher extends Thread {

    private SeqAct processor;

    private ArrayBlockingQueue<Msg> sharedQueue;

    private String _id;

    public Dispatcher(SeqAct p, ArrayBlockingQueue<Msg> q, String id) {
        processor = p;
        sharedQueue = q;
        _id = id;
    }

    @Override
    public void run() {
        log.info("( {} ) enter Dispatcher loop", _id);
        while (Unique.working.get() || sharedQueue.size() != 0) {
            Msg msg = null;
            try {
                msg = sharedQueue.take();
            } catch (InterruptedException ie) {
                log.error("( {} ) BlockQueue take data error - {}", _id, ie);
            }
            if (msg == null) continue;

            if (msg.head.equals("exit")) {
                break;
            }

            JSONObject jo = null;
            try {
                jo = JSON.parseObject(msg.body);
            } catch (JSONException je) {
                log.error("( {} ) JSON parse error - {}", _id, je);
                continue;
            }
            if (jo == null) continue;

            try {
                if (!processor.validate(jo)) {
                    continue;
                }
            } catch (RuntimeException re) {
                log.error("( {} ) validate failed - {}", _id, re);
                continue;
            }

            try {
                if (!processor.filter(jo)) {
                    continue;
                }
            } catch (RuntimeException re) {
                log.error("( {} ) filter operation error - {}", _id, re);
                continue;
            }

            try {
                processor.process(jo);
            } catch (RuntimeException re) {
                log.error("( {} ) process error - {}", _id, re);
            }
        }
        log.info("( {} ) exited Dispatcher process loop", _id);
    }

}
