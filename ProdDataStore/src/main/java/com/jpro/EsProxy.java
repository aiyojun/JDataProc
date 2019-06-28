package com.jpro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import lombok.var;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public class EsProxy implements DataAbstractProxy {
    private String uniqueKey;

    /**
     * post-fix of _id
     */
    private int IDMode = 1;

    private String secondField;

    private int tailBegin = 0;

    private int tailSize = 0;

    /**
     * maintain a buffer
     */
    private Queue<String> buffer;

    private int bufferMaxSize = 20;

    private Lock bufferLock = new ReentrantLock();

    /**
     * ES config
     */
    private String esIp;

    private int esPort;

    private String index;

    private String type;

    /**
     * monitor thread - store data to es
     */
    private boolean monitorTaskRunning = true;

    private int monitorTaskInterval = 5;

    /**
     * HTTP tools
     */
    private CloseableHttpClient httpclient;

    private Thread selfMonitorTask = new Thread(() -> {
        log.info("EsProxy monitor thread start");
        while (monitorTaskRunning) {
            try {
                Thread.sleep(monitorTaskInterval * 1000);
                int bulkCount = 0;
                if (!buffer.isEmpty()) {
                    bulkCount = esBulkWrite();
                }
                log.info("Execute one EsProxy bulk write, bulk count: " + bulkCount);
            } catch (Exception e) {
                log.error("EsProxy monitor thread err: " + e);
            }
        }
        log.info("EsProxy monitor thread exit.");
    });

    private void addToBuffer(String data) {
        bufferLock.lock();
        try {
            buffer.add(data);
        } catch (Exception ignored) {

        }
        bufferLock.unlock();
    }

    private void httpSend(String url, String body) {
        HttpPost postRequest = new HttpPost(url);
        StringEntity se;
        try {
            se = new StringEntity(body);
            se.setContentEncoding("UTF-8");
            se.setContentType("application/json");
            postRequest.setEntity(se);
            CloseableHttpResponse response = null;
            response = httpclient.execute(postRequest);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                // TODO
                log.info("Http success (bulk write) ! Response: " + response.getEntity().toString());
            } else {
                throw new RuntimeException("Network error, please check network.");
            }
        } catch (UnsupportedEncodingException e) {
            log.error("Bulk write failed (UnsupportedEncodingException): " + e);
        } catch (ClientProtocolException e) {
            log.error("Bulk write failed (ClientProtocolException): " + e);
        } catch (IOException e) {
            log.error("Bulk write failed (IOException): " + e);
        }
    }

    private int esBulkWrite() {
        int count = 0;
        StringBuilder requestBody = new StringBuilder();
        bufferLock.lock();
        try {
            String _id = null;
            for (String record: buffer) {
                // parse json string
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode root = mapper.readTree(record);
                    if (!root.has(uniqueKey)) {
                        throw new RuntimeException("No such unique field in data [ " + record + " ]");
                    }
                    if (IDMode == 2) {
                        if (!root.has(secondField)) {
                            throw new RuntimeException("No such second field in data [ " + record + " ]");
                        }
                        _id = root.path(uniqueKey).toString() + root.path(secondField).toString().substring(tailBegin, tailSize);
                    } else {
                        _id = root.path(uniqueKey).toString();
                    }
                } catch (Exception e) {
                    log.error("Parse json data failed: " + e);
                    continue;
                }

                requestBody.append("{ \"index\":  { \"_index\": \"")
                        .append(index)
                        .append("\", \"_type\": \"")
                        .append(type)
                        .append("\", \"_id\" : \"")
                        .append(_id)
                        .append("\" }}\n")
                        .append(record).append("\n");
                count++;
            }
            buffer.clear();
        } catch (Exception ignored) {

        }
        bufferLock.unlock();
        httpSend("http://" + esIp + ":" + esPort, requestBody.toString());
        return count;
    }

    @Override
    public void link(String ip, int port) {
        esIp = ip;
        esPort = port;
        buffer = new LinkedList<>();
        var props = GlobalContext.getInstence().getProperties();
        bufferMaxSize = new Integer(props.getProperty("store.es.buffer.size"));
        index = props.getProperty("store.es.index");
        type = props.getProperty("store.es.type");
        monitorTaskInterval = new Integer(props.getProperty("store.es.bulk.interval"));
        uniqueKey = props.getProperty("join.field");
        secondField = props.getProperty("join.field2");
        String post = props.getProperty("join.id.postfix.of.field2");
        IDMode = new Integer(props.getProperty("join.id.mode"));
        if (IDMode == 2) {
            if (!post.contains(":")) {
                log.warn("Invalid property [ join.id.postfix.of.field2 ] = [ " + post + " ], correct format: begin_position:string_size");
            } else {
                String[] list = post.split(":");
                tailBegin = new Integer(list[0]);
                tailSize  = new Integer(list[1]);
            }
        }

        // initialization
        RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(20).setConnectTimeout(20).setSocketTimeout(20).build();
        HttpClientBuilder builder = HttpClients.custom().setDefaultRequestConfig(requestConfig).setMaxConnTotal(50);
        httpclient = builder.build();

        // start
        selfMonitorTask.start();
    }

    @Override
    public void insert(String data) {
        addToBuffer(data);
    }

    @Override
    public void close() {
        monitorTaskRunning = false;
    }
}
