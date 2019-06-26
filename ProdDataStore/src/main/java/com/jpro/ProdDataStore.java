package com.jpro;

import lombok.extern.log4j.Log4j2;

import java.util.Properties;

@Log4j2
public class ProdDataStore {
	private static void checkProperty(Properties p, String key) {
		if (!p.containsKey(key)) {
			log.error("No such key [ kafka.ip ] in properties, exiting.");
			System.exit(-1);
		}
	}

	public static void main(String[] args) {
		/// 1. validate parameters from command line
		if (args.length != 1) {
			log.error("Usage: ProdDataStore [ config file ]");
			return;
		}
		final String configFile = args[0];

		log.info("program ProdDataStore start");

		/// 2. init global properties file
		GlobalContext.getInstence().load(configFile);
		Properties context = GlobalContext.getInstence().getProperties();

		/// 3. check necessary properties
		checkProperty(context, "kafka.ip");
		checkProperty(context, "kafka.port");
		checkProperty(context, "kafka.group.id");
		checkProperty(context, "kafka.topic");

		///-------------------------------------
		/// construct database proxy
		///-------------------------------------
		String databaseType = context.getProperty("store.db");
		DataAbstractProxy dataProxy;
		if (databaseType.equals("es")) {
			log.info("Database [ es ]");
			dataProxy = new EsProxy();
		} /*else if (databaseType.equals("mongo")) {
			log.info("Database [ mongo ]");
			 dataProxy = new MongProxy();
		}*/ else {
			log.info("No such key [ store.db ] in properties. Use database [ es ]");
			dataProxy = new EsProxy();
		}

		String databaseIp = context.getProperty("store.database.ip");
		String databasePort = context.getProperty("store.database.port");
		///-------------------------------------
		/// initialization operation
		///-------------------------------------
		dataProxy.link(databaseIp, new Integer(databasePort));

		///-------------------------------------
		/// construct KafkaConsumer & inject database proxy
		///-------------------------------------
		KafkaConsumer dataProcessor = new KafkaConsumer(dataProxy);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("Program exiting, close all resource of the program.");
			/// stop another thread, recycle thread resource.
			dataProcessor.stop();
			dataProxy.close();
		}));

		dataProcessor.start();
	}
}
