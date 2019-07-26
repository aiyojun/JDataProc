package com.jpro.frame;

import com.jpro.base.GlobalBloom;
import com.jpro.base.GlobalContext;
import com.jpro.base.JComToo;
import com.jpro.base.MongoProxy;
import lombok.extern.log4j.Log4j2;
import sun.misc.Signal;

import java.util.Properties;

@Log4j2
public class JoinCube {
	public static void main(String[] args) {
		if (args.length != 1) {
			log.error("Usage: JoinCube [ properties-file-path ]");
			return;
		}

//		Properties context = JComToo.getGlobalContext("JoinCube.properties");
		Properties context = JComToo.getGlobalContextF(args[0]);

		GlobalContext.getInstance().setContext(context);

		GlobalBloom.loadBloomFilterCondition();

		MongoProxy.getInstance().prepare();

		log.info("Program JoinCube start ...");

		/// TODO: all preparation
		Join join = new Join().prepare();
		Storage storage = new Storage().prepare();

		/// TODO: register events when program exiting
		Signal.handle(new Signal("INT"), signal -> {
			GlobalContext.isWorking.set(false);
			join.close();
			storage.close();
			MongoProxy.getInstance().close();
			log.info("Recycle resource.");
		});

		/// TODO: start all thread
		storage.start();
		join.start();

		log.info("All exiting ...");
//		System.exit(0);
	}
}
