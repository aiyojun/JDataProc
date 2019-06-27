package com.jpro;

import lombok.extern.log4j.Log4j2;
import lombok.var;

@Log4j2
public class SelfJoin {
	public static void main(String[] args) {
		/// 1. validate parameters from command line
		if (args.length != 1) {
			log.error("Usage: SelfJoin [ properties-file-path ]");
			return;
		}
		final String propertiesFile = args[0];

		log.info("Program SelfJoin start");

		/// 2. init global properties file
		GlobalContext ctx = GlobalContext.getInstence();
		ctx.load(propertiesFile);
		log.info("load global properties file: " + propertiesFile);
		log.info("Check necessary properties");

		/// 3. check necessary properties
		var props = ctx.getProperties();
		JComToo.checkProperty(props, "source.kafka.ip");

		/// 4. create main object
		AbstraFilter dataFilter = new LogicFilter();
		var core = new CoreProc(dataFilter);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("Recycle system resources");
			core.close();
		}));

		core.start();
	}
}
