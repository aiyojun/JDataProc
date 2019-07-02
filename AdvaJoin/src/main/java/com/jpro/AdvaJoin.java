package com.jpro;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import javax.print.Doc;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.mongodb.client.model.Filters.*;

@Log4j2
public class AdvaJoin {
	private static void justForTest() {
		log.info("just for test");

		Document doc = new Document();
		doc.forEach((k, v) -> {
			log.info("key: " + k + "\tvalue: " + v);
		});
		log.info("over");
		doc.append("hehe", "casdhbasd");
		doc.append("haha", 12);
		doc.forEach((k, v) -> {
			if (v instanceof String)
	 			log.info("string");
			log.info("key: " + k + "\tvalue: " + v);
		});
		System.exit(2);
//		FinalQuery

		try {
			MongoClient mongoClient = new MongoClient("172.16.1.244", 27017);
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mydb");
			log.info("connect to mongo db");
//			mongoDatabase.createCollection("jd_coll");
			MongoCollection<Document> collection = mongoDatabase.getCollection("jd_coll");
			Document document = new Document("name", "ming").append("age", 12).append("hobby", "swimming");
			List<Document> documents = new ArrayList<Document>();
			documents.add(document);
//			collection.insertMany(documents);
//			collection.insertOne(new Document("_id", "RSH0000066959143").append("name", "shan").append("hobby", "football"));
//			collection.updateOne(eq("_id", "RSH0000066959143"), new Document("$set", new Document("name","shang").append("hobby", "football")));
			FindIterable<Document> findIterable = collection.find(eq("_id", "RSH0000066959143"));
			MongoCursor<Document> mongoCursor = findIterable.iterator();
			int count = 0;
			if (mongoCursor.hasNext()) {
				count++;
			}
			log.info("count : " + count);
			log.info("insert success");
			collection.insertOne(new Document("_id", "RSH0000066959144_ST1").append("name", "dong").append("age", 24));
//			collection.findOneAndUpdate(eq("_id", "RSH000066959133"), new Document("$set", new Document("sister", "hong")));
		} catch (Exception e) {
			log.info("exception : " + e);
		}
		System.exit(2);
	}

	public static void main(String[] args) {
//		justForTest();
		/// 1. validate parameters from command line
		if (args.length != 1) {
			log.error("Usage: JoinTask [ properties-file-path ]");
			return;
		}
		Properties props = new Properties();
		try {
			File fp = new File(args[0]);
			if (!fp.exists())
				throw new RuntimeException("no properties file");
			FileInputStream inf = new FileInputStream(fp);
			props.load(inf);
		} catch (Exception e) {
			log.error("Load file failed: " + e);
			return;
		}
		log.info("Program AdvaJoin start");

		MooPoo mooPoo = new MooPoo(props.getProperty("mongo.ip"),
				Integer.parseInt(props.getProperty("mongo.port")),
				Integer.parseInt(props.getProperty("mongo.pool.size")));

		StoreAcces storeAcces = new StoreAcces(props);

		JoinTask joinTask = new JoinTask(props, storeAcces);
		SubsTask subsTask = new SubsTask(props, storeAcces);
//		RpcSrv rpcSrv     = new RpcSrv(props, mooPoo);

//		rpcSrv.init();
		joinTask.init();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//			rpcSrv.close();
			joinTask.close();
			subsTask.close();
			log.info("Recycle resources");
		}));

		Thread t1 = new Thread(joinTask::start);
		Thread t2 = new Thread(subsTask::start);

		t1.start();
		t2.start();

		log.info("Main Thread enter RPC loop");
//		rpcSrv.spin();

		log.info("Wait Thread-1 Thread-2");
	}
}
