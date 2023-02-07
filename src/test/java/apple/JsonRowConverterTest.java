package apple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.protojson.runner.JsonRowConverter;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2023-02-06
 */
public class JsonRowConverterTest {
    @Test
    public void testArray() throws IOException {
        String str0 = "[\"http://bd2.a.kwimgs"
                + ".com/upic/2022/12/17/17/BMjAyMjEyMTcxNzAwMDlfNTQwNjcxNzIzXzkxMzMyNDM3OTE1XzFfMw"
                + "==_ff_Bf62aa5a8e6c8f173818fc34a165a5824"
                + ".heif?tag=1-1674074347-d-0-egirchve8y-7df2338d7e55a200&type=hot&clientCacheKey=3xd5rkte7bgnnra_ff"
                + ".heif&di=13e9ede&bp=10001\",\"http://js2.a.yximgs"
                + ".com/upic/2022/12/17/17/BMjAyMjEyMTcxNzAwMDlfNTQwNjcxNzIzXzkxMzMyNDM3OTE1XzFfMw"
                + "==_ff_Bf62aa5a8e6c8f173818fc34a165a5824"
                + ".heif?tag=1-1674074347-d-1-sqdsymbuuc-8cc375b5b927a56f&type=hot&clientCacheKey=3xd5rkte7bgnnra_ff"
                + ".heif&di=13e9ede&bp=10001\"]";
        String str1 = "[{\"cdn\":\"txdwk.a.yximgs.com\",\"url\":\"http:\\/\\/txdwk.a.yximgs"
                + ".com\\/bs2\\/emotion\\/app_1574675492000_5xe88vmt2g36cxi.png\"},{\"cdn\":\"ali2.a.yximgs.com\","
                + "\"url\":\"http:\\/\\/ali2.a.yximgs.com\\/bs2\\/emotion\\/app_1574675492000_5xe88vmt2g36cxi"
                + ".png\"}]\n";
        String str2 = "";
        List<String> list = new ArrayList<>();
        list.add(str0);
        list.add(str2);
        list.add(str1);
        JsonRowConverter converter = new JsonRowConverter("0", "1", "1.cdn");
        for (String tmp : list) {
            try {
                String[] process = converter.process(tmp);
                for (String p : process) {
                    System.out.print(p + " ");
                }
            } catch (Exception e) {
                // ignore
                e.printStackTrace();
            } finally {
                System.out.println();
            }
        }
    }

    @Test
    public void testJson() throws IOException {
        String str = "{\"sdk_ver\":\"4.7.1.0\",\"config\":{\"max_retry_count\":1,\"bitmap_type\":\"RGB_565\","
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
        JsonRowConverter converter = new JsonRowConverter(
                "sdk_ver",
                "config",
                "config.max_disk_cache_size",
                "network.request",
                "network.requests",
                "network.requests",
                "network.requests.3",
                "extra_message",
                "bs_info.biz_ft",
                "bs_info.biz_extra",
                "network.requests.0",
                "network.requests.0.status",
                "network.requests.0.server_ip"
        );
        String[] process = converter.process(str);
        System.out.println(String.join("\n\n", process));
    }
}
