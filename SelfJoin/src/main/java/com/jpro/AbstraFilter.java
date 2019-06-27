package com.jpro;

import java.util.Map;

public interface AbstraFilter {
	Map<String, Object> filter(Map<String, Object> kv);
}