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
 * @author yehe <yehe@kuaishou.com>
 * Created on 2021-11-11
 */
public class ExtractRTCMixCallDuration extends UDF {
    public static ArrayList<String> evaluate(String json) throws IOException {
        JsonNode root = JsonUtils.parse(json);
        Iterator<Entry<String, JsonNode>> fields = root.fields();
        ArrayList<String> result = new ArrayList<>();
        HashMap<Long, Long> resolutionDurationMap = new HashMap<>();
        long duration = root.path("duration").asLong(0);
        long vHeight = root.path("v_height").asLong(0);
        long vWidth = root.path("v_width").asLong(0);
        if (duration <= 0) {
            return result;
        }
        long rtmp_para = 0;
        boolean isDirty = true;
        while (fields.hasNext()) {
            Entry<String, JsonNode> entry = fields.next();
            if (Pattern.matches("^p\\d+$", entry.getKey())) {
                Long height = entry.getValue().path("v_h").asLong(0l);
                Long width = entry.getValue().path("v_w").asLong(0l);
                long resolution = height * width;
                if (resolution == 0 && entry.getValue().path("a_bytes").asLong(0l) <= 0) {
                    continue;
                }
                if (resolutionDurationMap.containsKey(resolution)) {
                    resolutionDurationMap.put(resolution, resolutionDurationMap.get(resolution) + 1);
                } else {
                    resolutionDurationMap.put(resolution, 1l);
                }
            }
            if (Pattern.matches("^rtmp\\d+$", entry.getKey())) {
                rtmp_para++;
                if (entry.getValue().path("a_bytes").asLong(0) > 0) {
                    isDirty = false;
                }
            }
        }
        result.add(String.format("%d\t%d\t%s\t%s", rtmp_para, 0, "max_para", "rtmp"));
        // 通话时长(ms)#分辨率#类型#来源
        long totalResolution = 0;
        for (long k : resolutionDurationMap.keySet()) {
            if (k == 0) {
                result.add(String.format("%d\t0\t%s\t%s",resolutionDurationMap.get(k) * duration, "audio", "rtc"));
            } else {
                result.add(String.format("%d\t%d\t%s\t%s", resolutionDurationMap.get(k) * duration, k, "single_resolution", "rtc"));
            }
            totalResolution += k * resolutionDurationMap.get(k);
        }
        if (totalResolution > 0) {
            result.add(String.format("%d\t%d\t%s\t%s", duration, totalResolution, "total_resolution", "rtc"));
        }
        if (vHeight * vWidth > 0) {
            result.add(String.format("%d\t%d\t%s\t%s", duration, vHeight * vWidth, "single_resolution", "mix"));
        }
        else {
            if (!isDirty) {
                result.add(String.format("%d\t%d\t%s\t%s", duration, 0, "audio", "mix"));
            }
        }

        return result;
    }
}
