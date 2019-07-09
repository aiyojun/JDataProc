package com.jpro;

import lombok.extern.log4j.Log4j2;

import java.util.Properties;

@Log4j2
public class AdvaStor {
	public static void main(String[] args) {
		Properties props = new Properties();
		try {
			props.load(AdvaStor.class.getClassLoader().getResourceAsStream("data_storage.properties"));
		} catch (Exception e) {
			log.error("Load file failed: " + e);
			return;
		}
		log.info("program AdvaStor start ...");

		Porter porter = new Porter(props);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			porter.stop();
			log.info("Recycle resource.");
		}));
		porter.work();
	}
}
