package com.kuaishou.data.udf.video;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-09-01
 */
public class ParseImageRequestsNew extends UDF {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static List<String> evaluate(String json) {
        ArrayList<String> result = new ArrayList<>();
        try {
            ArrayNode node = (ArrayNode) MAPPER.readTree(json);
            for (JsonNode tmp : node) {
                result.add(MAPPER.writeValueAsString(tmp));
            }
        } catch (Exception e) {

        }
        return result;
    }

    public static void main(String[] args) {
        String str = "[{\"status\":\"success\",\"error_message\":\"\",\"url\":\"http://p4.a.yximgs"
                + ".com/uhead/AB/2022/05/15/06/BMjAyMjA1MTUwNjAwMDRfNDAzMDAxNDFfOTUyOTEyNDEwNV9sdg==.kpg\","
                + "\"http_code\":200,\"server_ip\":\"121.17.124.106\",\"protocol\":\"http/1.1\","
                + "\"received_bytes\":51889,\"cost\":57,\"dns_cost\":-1,\"connect_cost\":0,"
                + "\"waiting_response_cost\":31,\"response_cost\":17}]";
        System.out.println(evaluate(str).get(0));
    }
}
