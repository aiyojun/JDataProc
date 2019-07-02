package com.jpro;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import io.grpc.UniKeyQueReq;
import io.grpc.UniKeyQueResp;
import io.grpc.UniKeyQueSrvGrpc;
import io.grpc.stub.StreamObserver;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

@Log4j2
public class UniKeyQueImp extends UniKeyQueSrvGrpc.UniKeyQueSrvImplBase {
    private MooPoo mooPoo;

    UniKeyQueImp(MooPoo poo) {
        mooPoo = poo;
    }

    public void queryUniqueKey(UniKeyQueReq request, StreamObserver<UniKeyQueResp> responseObserver) {
        log.info("UniKeyQue Request UniKeyVal - " + request.getUniKeyVal());
        int count = 0;
        MongoClient mongoClient = mooPoo.getMongoClient();
        try {
            MongoCollection<Document> collection = mongoClient.getDatabase("mydb")
                    .getCollection("aim_data");
            FindIterable<Document> findIterable = collection.find(eq("_id", request.getUniKeyVal()));
            MongoCursor<Document> mongoCursor = findIterable.iterator();
            if (mongoCursor.hasNext()) count++;
        } catch (Exception e) {
            log.info(e);
        }
        mooPoo.returnMongoClient(mongoClient);
        UniKeyQueResp reps = UniKeyQueResp.newBuilder().setUniKeyExi(count != 0).build();
        responseObserver.onNext(reps);
        responseObserver.onCompleted();
    }
}
