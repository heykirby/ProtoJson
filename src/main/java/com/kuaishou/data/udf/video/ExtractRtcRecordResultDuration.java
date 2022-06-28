package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author <zhouwenjia@kuaishou.com>
 * Created on 2022-06-13
 */
public class ExtractRtcRecordResultDuration extends UDF{
    public static ArrayList<String> evaluate(String json) throws IOException {
        JsonNode root = JsonUtils.parse(json);
        Iterator<Entry<String, JsonNode>> fields = root.fields();
        ArrayList<String> result = new ArrayList<>();
        long duration = root.path("duration").asLong(0L);
        long vWight=root.path("v_wight").asLong(0L);
        long vHeight=root.path("v_height").asLong(0L);
        String mpuTp = root.path("mpu_tp").asText();
        boolean audioOnly=root.path("audio_only").asBoolean(false);

        String sourceType="mix";

        while(fields.hasNext()){
            Entry<String, JsonNode> entry = fields.next();
            //rtmp_stat列表中存在URL_TYPE为RECORD_URL的时候
            if (Pattern.matches("^rtmp\\d+$", entry.getKey())
                    && entry.getValue().path("url_type").equals("RECORD_URL")) {
                sourceType="record";
            }
        }
        // mpu_tp为audit时，值为audit
        if("audit".equals(mpuTp)) sourceType="audit";
        if(!audioOnly){   //audio_only不为纯音频，按照v_wight*v_height得到视频分辨率，rtc_type为single_resolution；
            result.add(String.format("%d\t%d\t%s\t%s", duration, vWight*vHeight, "single_resolution", sourceType));
            if(sourceType=="record") result.add(String.format("%d\t%d\t%s\t%s", duration, vWight*vHeight, "single_resolution", "mix"));
        }else{
            //audio_only为纯音频，则算一路音频时长，插入混流用量表；rtc_type为audio；
            result.add(String.format("%d\t%d\t%s\t%s", duration, 0, "audio", sourceType));
            if(sourceType=="record") result.add(String.format("%d\t%d\t%s\t%s", duration, 0, "audio", "mix"));
        }
        return result;
    }
}
