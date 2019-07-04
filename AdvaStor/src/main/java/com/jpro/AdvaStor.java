package com.jpro;

import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

@Log4j2
public class AdvaStor {
	public static void main(String[] args) {
		/// 1. validate parameters from command line
		if (args.length != 1) {
			log.error("Usage: AdvaStor [ properties-file-path ]");
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
		log.info("program AdvaStor start ...");

		Porter porter = new Porter(props);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			porter.stop();
			log.info("Recycle resource.");
		}));
		porter.work();
	}
}
