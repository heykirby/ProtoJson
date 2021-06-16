package com.kuaishou.data.udf.video;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author wuxuexin <wuxuexin@kuaishou.com>
 * Created on 2021-06-16
 */
public class KV2Json {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String evaluate(Object... objects) {
        try {
            int size = objects.length / 2;
            Map<String, Object> result = new HashMap<>();
            for (int i = 0; i < size; i++) {
                if (objects[2 * i + 1] != null) {
                    result.put(String.valueOf(objects[2 * i]), objects[2 * i + 1]);
                }
            }

            return MAPPER.writeValueAsString(result);
        } catch (Exception ignore) {
            return "{}";
        }
    }

    public static void main(String[] args) {
        KV2Json kv2Json = new KV2Json();
        System.out.println(kv2Json.evaluate("play_url", "www.123.com", "kwai_sign", null, "las_request_id", 123));
    }

}
