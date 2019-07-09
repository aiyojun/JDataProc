package com.jpro;

import lombok.extern.log4j.Log4j2;

import java.util.*;


@Log4j2
public class AdvaJoin2 {
	public static void main(String[] args) {
		Properties props = new Properties();
		try {
			props.load(AdvaJoin2.class.getClassLoader().getResourceAsStream("adva_join.properties"));
		} catch (Exception e) {
			log.error("Load file failed: " + e);
			return;
		}
		log.info("Program AdvaJoin start");

		MooPoo mooPoo = new MooPoo(props.getProperty("storage.mongo.ip"),
				Integer.parseInt(props.getProperty("storage.mongo.port")),
				Integer.parseInt(props.getProperty("storage.aim.pool.size")));

		Joinner joinTask = new Joinner(props, mooPoo);
		SubsTask subsTask = new SubsTask(props, mooPoo);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			joinTask.stop();
			subsTask.close();
			log.info("Recycle resources");
		}));

		Thread t1 = new Thread(subsTask::start);
		t1.start();

		joinTask.work();
	}
}
