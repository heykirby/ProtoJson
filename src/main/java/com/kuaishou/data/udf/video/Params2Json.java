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
        if (params.contains("=")) {
            if (params.contains("&"))
                return trans(params, "&");
            else
                return trans(params, ",");
        }
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
                "{}",
                "id=1153126868,llsid=2000153726805913330,exptag=1_i/2000153726805913330_scn0,browse_type=1,is_child_lock=false,business_type=UNKNOWN,ad_llsid=0,share_identify=false,is_long_video=false,paid_video=false,is_ad_feed=false,session_id=,is_full_screen=true,is_auto_play=false,profile_feed_on=false,photo_consume_page=photo,release_player_background=false,freeTailPlayDuration=0,status=false,tailoring_results=false,is_first_played_video=false,depth=0,full_screen_phone=false,black_matrix=0,subtitles=false,cut_shape=false,take_up_totally=false,immerse_style=false,is_preloaded=false",
                "{\"id\":\"1488987792\",\"h5_page\":\"\",\"exptag\":\"1_a/2000133657844043409_sl88$s$s\",\"photo_consume_page\":\"find\",\"utm_source\":\"\",\"is_clearscreen_play\":\"false\",\"is_child_lock\":\"false\",\"tailoring_results\":\"false\",\"is_auto_play\":\"false\",\"browse_type\":\"3\",\"profile_feed_on\":\"false\",\"is_ad_feed\":\"false\",\"llsid\":\"2000133657844043409\"}",
                "id=2119184430&llsid=2000133569215499009&exptag=1_u/2000133569215499009_p0$s&browse_type=3&is_child_lock=false&business_type=UNKNOWN&ad_llsid=0&share_identify=false&is_long_video=true&paid_video=false&is_ad_feed=false&is_full_screen=true&is_auto_play=false&profile_feed_on=false&photo_consume_page=find&release_player_background=false&freeTailPlayDuration=0&status=false&tailoring_results=false&is_first_played_video=false&depth=0&full_screen_phone=true&black_matrix=1&subtitles=true&cut_shape=false&take_up_totally=false&immerse_style=true&is_preloaded=false&preload_reason=null&launch_mode=1&author_is_living=true&screen_scale=19.0:9"
        };
        for (String params: paramsArr) {
            System.out.println(params2Json.evaluate(params));
        }
    }
}
