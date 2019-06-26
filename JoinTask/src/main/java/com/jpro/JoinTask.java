package com.jpro;

import lombok.extern.log4j.Log4j2;
import lombok.var;

@Log4j2
public class JoinTask {
	public static void main(String[] args) {
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

		if (props.getProperty("stream.filter.switch").equals("true")) {
			log.info("Look out! you opened [ filter mechanism ] .");
		}
		log.info("Join field: " + props.getProperty("join.field"));
		if (props.getProperty("join.demensions").equals("2")) {
			log.info("Join Mode: [ 1 - Kafka Stream ] + [ 2 - ES Tables ]");
		}
		log.info(ctx.getProperties().getProperty("stream.main0.kafka.topic"));

		log.info("Main stream of Kafka ip:port - "
				+ props.getProperty("kafka.ip") + ":" + props.getProperty("kafka.port"));
		log.info("Main stream of Kafka group id - " + props.getProperty("stream.main0.group.id"));
		log.info("Output es ip:port - "
				+ props.getProperty("output.es.ip") + ":" + props.getProperty("output.es.port"));
		log.info("Output es index/type - "
				+ props.getProperty("output.es.index") + "/" + props.getProperty("output.es.type"));

		Joinner joinner = new Joinner();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			joinner.close();
			log.info("Recycle resources, exit program.");
		}));
		joinner.main();
	}
}
