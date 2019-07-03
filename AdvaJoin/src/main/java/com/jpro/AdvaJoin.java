package com.jpro;

import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

@Log4j2
public class AdvaJoin {
	public static void main(String[] args) {
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

		StoreAcces storeAcces = new StoreAcces(props);

		JoinTask joinTask = new JoinTask(props, storeAcces);
		SubsTask subsTask = new SubsTask(props, storeAcces);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			joinTask.close();
			subsTask.close();
			log.info("Recycle resources");
		}));

		Thread t1 = new Thread(joinTask::start);
		t1.start();

		subsTask.start();
	}
}
