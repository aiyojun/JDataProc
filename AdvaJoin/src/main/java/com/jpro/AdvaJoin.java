package com.jpro;

import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

		Map<String, String> stationsAliasMapping = new HashMap<>();
		Map<String, String> stationsOwnerMapping = new HashMap<>();
		{ // read station info, parse string array.
			List<String> stations = ComToo.parseArrayString(props.getProperty("station.value"));
			List<String> stationsAlias = ComToo.parseArrayString(props.getProperty("station.alias"));
			List<String> stationsOwner = ComToo.parseArrayString(props.getProperty("station.owner"));
			if (stations.size() != stationsAlias.size() || stations.size() != stationsOwner.size()) {
				log.error("Check properties file [ station.value - station.alias - station.owner ]");
				System.exit(3);
			}
			for (int i = 0; i < stations.size(); i++) {
				stationsAliasMapping.put(stations.get(i), stationsAlias.get(i));
				stationsOwnerMapping.put(stations.get(i), stationsOwner.get(i));
			}
		}

		JoinTask joinTask = new JoinTask(props, storeAcces, stationsAliasMapping, stationsOwnerMapping);
		SubsTask subsTask = new SubsTask(props, storeAcces, stationsAliasMapping, stationsOwnerMapping);

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
