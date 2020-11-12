package com.kuaishou.data.udf.video;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kuaishou.data.udf.video.utils.JsonUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

public class Params2Json extends UDF {
    private final ObjectMapper MAPPER = JsonUtils.getMapper();

    public String evaluate(String params) throws JsonProcessingException {
        try {
            if (params.contains("=")) {
                if (params.contains("&"))
                    return trans(params, "&");
                else
                    return trans(params, ",");
            }
        } catch (Exception ignore) { return "{}";}
        return params;
    }

    private String trans(String params, String splitter) throws JsonProcessingException {
        String[] kvs = params.split(splitter);
        Map<String, String> KV_MAP = new HashMap<>();
        for (String kv: kvs) {
            String KV_SPLITTER = "=";
            String[] pair = kv.split(KV_SPLITTER);
            String value = "";
            if (pair.length > 1) value = pair[1];
            KV_MAP.put(pair[0], value);
        }
        return MAPPER.writeValueAsString(KV_MAP);
    }

    public static void main(String[] args) throws JsonProcessingException {
        Params2Json params2Json = new Params2Json();

        String[] paramsArr = {
                "{\"query_name\":\"4414¥,=,_,¥=,=¥\\/4788858928116557774\",\"query_vertical_type\":\"SEARCH_PAGE\",\"query_id\":\"NDQxNMKlLD0sXyzCpT0sPcKlLzQ3ODg4NTg5MjgxMTY1NTc3NzQxNDE2NzY5OTMxNjA1MDcyMzc3Ljg5MjEwNDc1Mjc=\",\"entry_source\":\"search_entrance_home\",\"query_source_type\":\"USER_INPUT\"}"
        };
        for (String params: paramsArr) {
            System.out.println(params2Json.evaluate(params));
        }
    }
}
