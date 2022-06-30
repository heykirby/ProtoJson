package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author <zhouwenjia@kuaishou.com>
 * Created on 2022-06-08
 */
public class JaimeReportPullProcessor extends UDF {
    public static ArrayList<String> evaluate(String json) throws IOException {
        JsonNode root = JsonUtils.parse(json);
        Iterator<Entry<String, JsonNode>> fields = root.fields();
        ArrayList<String> result = new ArrayList<>();
        // 视频和音频 <resolution, duration>
        HashMap<Long,Long> videoDurationMap = new HashMap<>();
        // 音频 pid Set
        Set<String> audioPidSet = new HashSet<>();
        //视频 pid Set
        Set<String> videoPidSet = new HashSet<>();
        long duration = root.path("duration").asLong(0);
        String mpuTp = root.path("mpu_tp").asText();
        boolean isRecord = false;
        int sourceType = -1;

        while(fields.hasNext()){
            Entry<String, JsonNode> entry = fields.next();
            if (Pattern.matches("^rtmp\\d+$", entry.getKey())
                    && entry.getValue().path("url_type").asText().equals("RECORD_URL")) {
                isRecord = true;
            }
            if (Pattern.matches("^p\\d+$", entry.getKey())) {
                String pid = entry.getValue().path("pid").asText("UNKNOWN");
                Long height = entry.getValue().path("v_h").asLong(0L);
                Long width = entry.getValue().path("v_w").asLong(0L);
                long resolution = height * width;
                if(resolution > 0) {
                    videoDurationMap.put(resolution, videoDurationMap.getOrDefault(resolution, 0L) + 1);
                    videoPidSet.add(pid);
                } else {
                    audioPidSet.add(pid);
                }
            }
        }
        audioPidSet.removeAll(videoPidSet);
        sourceType = "audit".equals(mpuTp) ? 3 : (isRecord ? 4 : 2);

        if(!videoPidSet.isEmpty()) {
            long totalResolution = 0L;
            // v_w*v_h为分辨率，如果值大于0，为视频数据,插入用量表；rtc_type为single_resolution；
            for (long re : videoDurationMap.keySet()) {
                totalResolution += videoDurationMap.get(re) * duration;
                result.add(String.format("%d\t%d\t%s\t%s", videoDurationMap.get(re) * duration, re, "single_resolution", sourceType));
            }
            if (!audioPidSet.isEmpty()) {
                result.add(String.format("%d\t%d\t%s\t%s", audioPidSet.size() * duration, 0, "single_audio", sourceType));
            }
            result.add(String.format("%d\t%d\t%s\t%s", duration, totalResolution, "total_resolution", sourceType));
        } else{
            result.add(String.format("%d\t%d\t%s\t%s", duration, 0, "single_audio", sourceType));
            result.add(String.format("%d\t%d\t%s\t%s", duration, 0, "audio", sourceType));
        }
        return result;
    }
}
