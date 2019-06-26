package com.jpro;

import lombok.extern.log4j.Log4j2;
import lombok.var;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@Log4j2
public class JoinTask {
	private static void doSomething(List<String> li) {
		li.remove(li.size() - 1);
	}

	private static void test() {
		List<String> li = new LinkedList<>();
		li.add("aaa");
		li.add("bbb");
		li.add("ccc");
//		var seq = JComToo.parseCsv("serial_nn ,name,age, hobbyee");
		for (var p : li) {
			log.info("--- p: " + p);
		}
		doSomething(li);
		log.info("---");
		for (var p : li) {
			log.info("--- p: " + p);
		}

		if (li.contains("ccc")) {
			log.info("exist aaa");
		} else {
			log.info("not exist aaa");
		}
	}

	private static void justForTest() {
//		RestClientBuilder builder = new RestClientBuilder();
		log.info("start es testing ...");
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("172.16.1.124", 9200, "http"))
		);
		GetRequest request = new GetRequest("jd_idx_t", "jd_tp", "Rhi5JWsBTuYKt95uzLoL");
		try {
			GetResponse resp = client.get(request);
			log.info(resp);
		} catch (IOException e) {
			log.error("get IOException: " + e);
		}

		System.exit(0);
	}
	public static void main(String[] args) {
		justForTest();
		/// 1. validate parameters from command line
		if (args.length != 1) {
			log.error("Usage: JoinTask [ properties-file-path ]");
			return;
		}
		final String propertiesFile = args[0];

		log.info("Program JoinTask start");

		/// 2. init global properties file
		GlobalContext ctx = GlobalContext.getInstence();
		ctx.load(propertiesFile);
		log.info("load global properties file: " + propertiesFile);
		log.info("Check necessary properties");

		var props = ctx.getProperties();
		JComToo.checkProperty(props, "stream.filter.switch");
		JComToo.checkProperty(props, "join.field");
		JComToo.checkProperty(props, "join.demensions");
		JComToo.checkProperty(props, "stream.main0.kafka.topic");

		JComToo.checkProperty(props, "kafka.ip");
		JComToo.checkProperty(props, "kafka.port");
		JComToo.checkProperty(props, "stream.main0.group.id");

		JComToo.checkProperty(props, "output.es.ip");
		JComToo.checkProperty(props, "output.es.port");
		JComToo.checkProperty(props, "output.es.index");
		JComToo.checkProperty(props, "output.es.type");

		log.info(ctx.getProperties().getProperty("stream.filter.switch"));
		log.info(ctx.getProperties().getProperty("stream.main0.filter.fields"));
		log.info(ctx.getProperties().getProperty("stream.line1.filter.fields"));
		log.info(ctx.getProperties().getProperty("stream.line2.filter.fields"));
		log.info(ctx.getProperties().getProperty("join.field"));
		log.info(ctx.getProperties().getProperty("join.demensions"));
		log.info(ctx.getProperties().getProperty("stream.main0.kafka.topic"));

		log.info(ctx.getProperties().getProperty("kafka.ip"));
		log.info(ctx.getProperties().getProperty("kafka.port"));
		log.info(ctx.getProperties().getProperty("stream.main0.group.id"));

		log.info(ctx.getProperties().getProperty("output.es.ip"));
		log.info(ctx.getProperties().getProperty("output.es.port"));
		log.info(ctx.getProperties().getProperty("output.es.index"));
		log.info(ctx.getProperties().getProperty("output.es.type"));

		Joinner joinner = new Joinner();
//		joinner.main();
	}
}
