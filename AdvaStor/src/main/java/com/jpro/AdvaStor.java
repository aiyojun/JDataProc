package com.jpro;

import lombok.extern.log4j.Log4j2;
import org.bson.Document;

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

		MooPoo AimMoo = new MooPoo(props.getProperty("mongo.aim.ip"), Integer.parseInt(props.getProperty("mongo.aim.port")), Integer.parseInt(props.getProperty("mongo.aim.pool.size")));
		MooPoo CncMoo;
		if (props.getProperty("stream.type").equals("sn")) {
			CncMoo = new MooPoo(props.getProperty("mongo.sn.ip"), Integer.parseInt(props.getProperty("mongo.sn.port")), Integer.parseInt(props.getProperty("mongo.sn.pool.size")));
		} else {
			CncMoo = new MooPoo(props.getProperty("mongo.cnc.ip"), Integer.parseInt(props.getProperty("mongo.cnc.port")), Integer.parseInt(props.getProperty("mongo.cnc.pool.size")));
		}
		MooPoo ExcMoo = new MooPoo(props.getProperty("mongo.exc.ip"), Integer.parseInt(props.getProperty("mongo.exc.port")), Integer.parseInt(props.getProperty("mongo.exc.pool.size")));

		SpecStor specStor = new SpecStor(props, ExcMoo, AimMoo, CncMoo);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			specStor.close();
			log.info("Recycle resource.");
		}));
		if (props.getProperty("stream.type").equals("sn")) {
			specStor.startForSN();
		} else {
			specStor.start();
		}
	}
}
