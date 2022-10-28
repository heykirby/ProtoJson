package org.protojson.test;

import java.io.IOException;

import org.protojson.runner.JsonRowConverter;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-10-27
 */

public class JsonRowConverterTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @Test
    public void testPerformance() throws IOException {
        String json = "{\"sdk_ver\":\"4.7.1.0\",\"config\":{\"max_retry_count\":1,\"bitmap_type\":\"RGB_565\","
                + "\"max_decoded_mem_cache_size\":74658610,\"max_encoded_mem_cache_size\":4194304,"
                + "\"max_disk_cache_size\":52428800},\"options\":{\"url\":\"http://p4.a.yximgs"
                + ".com/uhead/AB/2022/05/15/06/BMjAyMjA1MTUwNjAwMDRfNDAzMDAxNDFfOTUyOTEyNDEwNV9sdg==.kpg\","
                + "\"urls\":[],\"view_exists\":true,\"view_width\":413,\"view_height\":538,\"ratio\":1},"
                + "\"meta\":{\"format\":\"KPG\",\"size\":1808640,\"width\":720,\"height\":1256,\"frame_count\":1},"
                + "\"stat\":{\"status\":\"success\",\"data_source\":\"network\",\"error_message\":\"\","
                + "\"first_screen\":208,\"stay_duration\":515},\"cache\":{\"cost\":27,"
                + "\"decoded_mem_cached_count\":51,\"decoded_mem_cached_size\":73667368,"
                + "\"encoded_mem_cached_count\":100,\"encoded_mem_cached_size\":4126594,\"disk_cached_count\":503,"
                + "\"disk_cached_size\":23406114},\"network\":{\"status\":\"success\",\"cost\":66,\"url\":\"http://p4"
                + ".a.yximgs.com/uhead/AB/2022/05/15/06/BMjAyMjA1MTUwNjAwMDRfNDAzMDAxNDFfOTUyOTEyNDEwNV9sdg==.kpg\","
                + "\"server_ip\":\"121.17.124.106\",\"host\":\"p4.a.yximgs.com\",\"error_message\":\"\","
                + "\"retry_count\":0,\"http_code\":200,\"requests\":[{\"status\":\"success\",\"error_message\":\"\","
                + "\"url\":\"http://p4.a.yximgs"
                + ".com/uhead/AB/2022/05/15/06/BMjAyMjA1MTUwNjAwMDRfNDAzMDAxNDFfOTUyOTEyNDEwNV9sdg==.kpg\","
                + "\"http_code\":200,\"server_ip\":\"121.17.124.106\",\"protocol\":\"http/1.1\","
                + "\"received_bytes\":51889,\"cost\":57,\"dns_cost\":-1,\"connect_cost\":0,"
                + "\"waiting_response_cost\":31,\"response_cost\":17}]},\"decode\":{\"status\":\"success\","
                + "\"cost\":64,\"width\":1280,\"height\":720,\"bitmap_type\":\"RGB_565\"},"
                + "\"bs_info\":{\"biz_ft\":\"video\",\"biz_extra\":{\"sub_solution\":\"KRN\","
                + "\"bundle_id\":\"xxxxxxxxxxx\",\"up_biz_ft\":\"FT_Feed\"},\"scene\":\"feed_cover\"},"
                + "\"sys_prof\":{\"in_background\":false,\"mem_usage\":0.1311},"
                + "\"extra_message\":{\"controller_id\":\"163\",\"request_id\":\"162\"}}";
        String[] result = new String[10];
        long startTime, endTime;
        long retryTime = 1000000;
        startTime = System.currentTimeMillis();
        for (int i = 0; i < retryTime; i++) {
            JsonNode node = MAPPER.readTree(json);
            result[0] = node.path("stat").path("status").asText();
            result[1] = node.path("stat").path("first_screen").asText();
            result[2] = node.path("stat").path("stay_duration").asText();
            result[3] = node.path("network").path("cost").asText();
            // result[4] = MAPPER.writeValueAsString(node.path("network").path("requests"));
            result[5] = node.path("decode").path("bitmap_type").asText();
            result[6] = MAPPER.writeValueAsString(node.path("bs_info").path("biz_extra"));
            result[7] = node.path("bs_info").path("biz_extra").path("bundle_id").asText();
            result[8] = node.path("sys_prof").path("in_background").asText();
            result[9] = MAPPER.writeValueAsString(node.path("extra_message"));
        }
        endTime = System.currentTimeMillis();
        System.out.println(String.join(",", result));
        System.out.println(endTime - startTime);

        startTime = System.currentTimeMillis();
        JsonRowConverter converter = new JsonRowConverter("stat.status", "stat.first_screen", "stat.stay_duration",
                "network.cost", "network.requests", "decode.bitmap_type", "bs_info.biz_extra", "bs_info.biz_extra.bundle_id",
                "sys_prof.in_background", "extra_message");
        for (int i = 0; i < retryTime; i++) {
            result = converter.process(json);
        }
        endTime = System.currentTimeMillis();
        System.out.println(String.join(",", result));
        System.out.println(endTime - startTime);
    }
}
