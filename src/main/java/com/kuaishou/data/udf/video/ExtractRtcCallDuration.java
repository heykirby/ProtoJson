package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author yehe <yehe@kuaishou.com>
 * Created on 2021-09-08
 */
public class ExtractRtcCallDuration extends UDF {
    public static ArrayList<String> evaluate(String json) throws IOException {
        JsonNode node = JsonUtils.parse(json).path("details").path("down");
        ArrayList<String> result = new ArrayList<>();
        if (node.has("v") && node.path("v").size() > 0) {
            Iterator<Entry<String, JsonNode>> iterator = node.path("v").fields();
            HashMap<Long, Integer> resolutionMap = new HashMap<>();
            while (iterator.hasNext()) {
                JsonNode single = iterator.next().getValue();
                long resolution = single.path("width").asInt(0) * single.path("height").asInt(0);
                if (resolutionMap.containsKey(resolution)) {
                    resolutionMap.put(resolution, resolutionMap.get(resolution) + 1);
                }
                resolutionMap.put(resolution, 1);
            }
            if (resolutionMap.size() == 0) {
                return result;
            }
            int totalResolution = 0;
            for (long k : resolutionMap.keySet()) {
                result.add(String.format("%d\t%d\t%s", resolutionMap.get(k), k, "single_resolution"));
                totalResolution += k * resolutionMap.get(k);
            }
            result.add(String.format("1\t%d\t%s", totalResolution, "total_resolution"));
        } else {
            result.add(String.format("1\t0\t%s", "audio"));
        }
        return result;
    }
}
