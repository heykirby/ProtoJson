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
 * Created on 2022-06-08
 */
public class ExtractRtcRecordDuration extends UDF {
    public static ArrayList<String> evaluate(String json) throws IOException {
        JsonNode root = JsonUtils.parse(json);
        Iterator<Entry<String, JsonNode>> fields = root.fields();
        ArrayList<String> result = new ArrayList<>();
        // 音频视频计数Map
        HashMap<Long,Long> resolutionDurationMap=new HashMap<>();
        long duration = root.path("duration").asLong(0);
        String mpuTp = root.path("mpu_tp").asText();
        int sourceType=2;
        //视频用量的List
        ArrayList<Long> videoList=new ArrayList<>();
        while(fields.hasNext()){
            Entry<String, JsonNode> entry = fields.next();
            if (Pattern.matches("^rtmp\\d+$", entry.getKey())
                    && entry.getValue().path("url_type").asText().equals("RECORD_URL")) {
                sourceType=4;
            }
            if (Pattern.matches("^p\\d+$", entry.getKey())) {
                Long height = entry.getValue().path("v_h").asLong(0l);
                Long width = entry.getValue().path("v_w").asLong(0l);
                long resolution = height * width;
                if(resolution!=0) videoList.add(resolution);
                resolutionDurationMap.put(resolution,resolutionDurationMap.getOrDefault(resolution,0L)+1);
            }
        }
        if("audit".equals(mpuTp)) sourceType=3;


        if(!videoList.isEmpty()) {
            // v_w*v_h为分辨率，如果值大于0，为视频数据,插入用量表；rtc_type为single_resolution；
            for (Long re : resolutionDurationMap.keySet()) {
                if (re!=0L)
                    result.add(String.format("%d\t%d\t%s\t%s", resolutionDurationMap.get(re)*duration, re, "single_resolution", sourceType));
            }

            if(resolutionDurationMap.containsKey(0L))
                result.add(String.format("%d\t%d\t%s\t%s", resolutionDurationMap.get(0L)*duration, 0, "single_audio", sourceType));
        } else{
            result.add(String.format("%d\t%d\t%s\t%s", duration, 0, "single_audio", sourceType));
        }

        //集合分辨率
        //v_w*v_h为分辨率，如果值大于0，为视频数据,插入用量表；rtc_type为total_resolution；
        if(!videoList.isEmpty()) {
            Long videoCount=0L;
            for (Long video : videoList) {
                videoCount+=video;
            }
            result.add(String.format("%d\t%d\t%s\t%s", duration, videoCount, "total_resolution", sourceType));
        } else{  // 当无视频数据时；按照音频算一路时长,rtc_type为total_audio
            result.add(String.format("%d\t%d\t%s\t%s", duration, 0, "total_audio", sourceType));
        }

        return result;
    }
}
