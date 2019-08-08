package com.jpro;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.log4j.Log4j2;
import sun.misc.Signal;

import java.util.concurrent.ArrayBlockingQueue;

import static com.jpro.Unique.Utils.gtv;
import static com.jpro.Unique.elfin;

@Log4j2
public class FullInspection {
	public static void test() {
		String s = "{\"key\":12}";
		JSONObject jo = JSON.parseObject(s);
		System.out.println(jo.toString());
		Object o = jo.get("key2");
		if (o instanceof Integer) {
			System.out.println("ohhhh~~~");
		} else {
			if (o == null) {
				System.out.println("null");
			}
			System.out.println("nonono~~~~~~~~~~");
		}
		System.exit(2);
	}

	public static void main(String[] args) {
//		test();
		System.out.println(
			" ____  ____  ____  ____  ____    __   ____  ____  _____  _  _ \n" +
			"( ___)( ___)(  _ \\( ___)(  _ \\  /__\\ (_  _)(_  _)(  _  )( \\( )\n" +
			" )__)  )__)  )(_) ))__)  )   / /(__)\\  )(   _)(_  )(_)(  )  ( \n" +
			"(__)  (____)(____/(____)(_)\\_)(__)(__)(__) (____)(_____)(_)\\_)\n"
		);
		log.info("program FullInspection start ...");
		Unique.initializeAllResource(args);

		Signal.handle(new Signal("INT"), sig -> {
			log.info("recv INT sig");
			Unique.recycleAllResource();
		});

		Unique.working.set(true);

		new Server().prepare().start();
	}
}
