package com.jpro.frame;

import com.jpro.base.JComToo;
import com.jpro.base.MongoSingle;
import lombok.extern.log4j.Log4j2;

import java.util.Properties;

@Log4j2
public class JoinCube {
	public static void main(String[] args) {
		Properties context = JComToo.getGlobalContext("JoinCube.properties");
		MongoSingle.getInstance().setContext(context);

		log.info("Program JoinCube start ...");

		/// TODO: all preparation
		Join join = new Join(context).prepare();
		Storage storage = new Storage(context).prepare();

		/// TODO: register events when program exiting
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			join.close();
			storage.close();
			log.info("Recycle resource.");
		}));

		/// TODO: start all thread
		storage.start();
		join.start();
	}
}
