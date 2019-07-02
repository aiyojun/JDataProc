package com.jpro;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.util.Properties;

@Log4j2
public class RpcSrv {
    private Properties gProps;

    private MooPoo mooPoo;

    private Server server;

    private UniKeyQueImp uniKeyQueImp;

    RpcSrv(Properties props, MooPoo poo) {
        gProps = props;
        mooPoo = poo;
    }

    public void close() {
        server.shutdown();
    }

    public void init() {
        try {
            uniKeyQueImp = new UniKeyQueImp(mooPoo);
            server = ServerBuilder.forPort(50051).addService(uniKeyQueImp).build().start();
            log.info("RPC service init port - 50051");
        } catch (IOException e) {
            log.error(e);
        }
    }

    public void spin() {
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            log.error(e);
        }
    }
}
