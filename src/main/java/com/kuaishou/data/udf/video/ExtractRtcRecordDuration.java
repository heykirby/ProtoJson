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
        HashMap<Long,Long> resolutionDurationMap=new HashMap<>();
        long duration = root.path("duration").asLong(0);
//        long vHeight = root.path("v_height").asLong(0);
//        long vWidth = root.path("v_width").asLong(0);
        String mpuTp = root.path("mpu_tp").asText();
        int sourceType=2;
        //视频用量的中间List
        ArrayList<Long> res=new ArrayList<>();
        while(fields.hasNext()){
            Entry<String, JsonNode> entry = fields.next();
            if (Pattern.matches("^rtmp\\d+$", entry.getKey())
                    && entry.getValue().path("url_type").equals("RECORD_URL")) {
                sourceType=4;
            }
            if (Pattern.matches("^p\\d+$", entry.getKey())) {
                Long height = entry.getValue().path("v_h").asLong(0l);
                Long width = entry.getValue().path("v_w").asLong(0l);
                long resolution = height * width;
                if(resolution!=0) res.add(resolution);
                resolutionDurationMap.put(resolution,resolutionDurationMap.getOrDefault(resolution,0L)+1);
            }
        }
        if(mpuTp.equals("audit")) sourceType=3;
        String type=sourceType==3?"audit":"mix";
        long totalResolution=0;

        // v_w*v_h为分辨率，如果值大于0，为视频数据,插入用量表；rtc_type为single_resolution；
        for (Long re : res) {
            result.add(String.format("%d\t%d\t%s\t%s", duration, re*duration, "single_resolution", type));
            if(sourceType==4){
                result.add(String.format("%d\t%d\t%s\t%s", duration, re*duration, "single_resolution", "record"));
            }
        }
        // 如果值等于0或者为空，为音频数据,累加插入用量表；rtc_type为single_audio
        if(resolutionDurationMap.containsKey(0L)) {
            result.add(String.format("%d\t%d\t%s\t%s", duration, resolutionDurationMap.get(0)*duration, "single_audio", type));
            if(sourceType==4) result.add(String.format("%d\t%d\t%s\t%s", duration, resolutionDurationMap.get(0)*duration, "single_audio", "record"));
        }
        // 当无视频数据时；按照音频算一路时长,rtc_type为single_audio
        if(res.size()==0) {
            result.add(String.format("%d\t%d\t%s\t%s", duration, duration, "single_audio", type));
            if(sourceType==4) result.add(String.format("%d\t%d\t%s\t%s", duration, duration, "single_audio", "record"));
        }

        //集合分辨率
        //v_w*v_h为分辨率，如果值大于0，为视频数据,插入用量表；rtc_type为total_resolution；
        for (Long re : res) {
            result.add(String.format("%d\t%d\t%s\t%s", duration, re*duration, "total_resolution", type));
            if(sourceType==4){
                result.add(String.format("%d\t%d\t%s\t%s", duration, re*duration, "total_resolution", "record"));
            }
        }
        // 当无视频数据时；按照音频算一路时长,rtc_type为total_audio
        if(res.size()==0) {
            result.add(String.format("%d\t%d\t%s\t%s", duration, duration, "total_audio", type));
            if(sourceType==4) result.add(String.format("%d\t%d\t%s\t%s", duration, duration, "total_audio", "record"));
        }
        return result;
    }
}
