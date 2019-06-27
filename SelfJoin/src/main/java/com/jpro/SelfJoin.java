package com.jpro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.extern.log4j.Log4j2;
import lombok.var;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Log4j2
public class SelfJoin {
	private static void justForTest() {
		String jsonStr = "{\"name\":\"ming\",\"age\":23,\"has\":false,\"id\":12.1}";
		try {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode root = mapper.readTree(jsonStr);
			Map<String, Object> _r = new HashMap<>();
			Map<String, Object> newr = new HashMap<>();
			for (var iter = root.fields(); iter.hasNext();) {
				var ele = iter.next();
				_r.put(ele.getKey(), ele.getValue());
//				if (ele.getKey().equals("age")) {
//					continue;
//				}
//				newr.put(ele.getKey(), ele.getValue());
			}
			_r.forEach((key, value) -> {
				if (value instanceof TextNode) {
					String sss = ((TextNode) value).textValue();
					log.info("sss: " + sss + "\t type: ");
					newr.put(key, ((TextNode) value).asText());
				} else if (value instanceof DoubleNode) {
					double sss = ((DoubleNode) value).asDouble();
					log.info("double type : " + sss);
					newr.put(key, ((DoubleNode) value).asDouble());
				} else if (value instanceof IntNode) {
					newr.put(key, ((IntNode) value).asInt());
				}
				log.info("key: " + key + "\tvalue: " + value + "\t value type: " + value.getClass().getName());
			});
			log.info("-----------------------");
			newr.forEach((key, value) -> {
				if (value instanceof TextNode) {
					newr.put(key, ((TextNode) value).asText());
				} else if (value instanceof DoubleNode) {
					newr.put(key, ((DoubleNode) value).asDouble());
				} else if (value instanceof IntNode) {
					newr.put(key, ((IntNode) value).asInt());
				}
				log.info("key: " + key + "\tvalue: " + newr.get(key) + "\t value type: " + newr.get(key).getClass().getName());
			});
			String result = mapper.writeValueAsString(newr);

			log.info("result: " + result);
		} catch (Exception e) {
			log.error(e);
		}
		System.exit(2);
	}

	public static void main(String[] args) {
		justForTest();
		/// 1. validate parameters from command line
		if (args.length != 1) {
			log.error("Usage: SelfJoin [ properties-file-path ]");
			return;
		}
		final String propertiesFile = args[0];

		log.info("Program SelfJoin start");

		/// 2. init global properties file
		GlobalContext ctx = GlobalContext.getInstence();
		Properties props = ctx.getProperties();
		ctx.load(propertiesFile);
		log.info("load global properties file: " + propertiesFile);
		log.info("Check necessary properties");

		/// 3. check necessary properties
		JComToo.checkProperty(props, "source.kafka.ip");

		/// 4. create main object
		AbstraFilter dataFilter = new LogicFilter(props.getProperty("filter.logic.field"), null);
		var core = new CoreProc(dataFilter);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("Recycle system resources");
			core.close();
		}));

		core.start();
	}
}
