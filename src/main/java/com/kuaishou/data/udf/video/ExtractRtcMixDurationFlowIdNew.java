package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-05-18
 */
public class ExtractRtcMixDurationFlowIdNew extends UDF {
    public static ArrayList<String> evaluate(String json) throws IOException {
        JsonNode root = JsonUtils.parse(json);
        Iterator<Entry<String, JsonNode>> fields = root.fields();
        ArrayList<String> result = new ArrayList<>();
        long duration = root.path("duration").asLong(0);
        if (duration <= 0) {
            return result;
        }
        TreeMap<String, HashMap<Long, Long>> map = new TreeMap<>();
        while (fields.hasNext()) {
            Entry<String, JsonNode> entry = fields.next();
            if (Pattern.matches("^p\\d+$", entry.getKey())) {
                String pid = entry.getValue().path("pid").asText("unknown");
                Long height = entry.getValue().path("v_h").asLong(0l);
                Long width = entry.getValue().path("v_w").asLong(0l);
                long resolution = height * width;
                if (!map.containsKey(pid)) {
                    map.put(pid, new HashMap<>());
                }
                map.get(pid).put(resolution, map.get(pid).getOrDefault(resolution, 0l) + 1);
            }
        }
        if (map.size() == 0) {
            return result;
        }
        int totalResolution = 0;
        for (String pid : map.keySet()) {
            for (long resolution : map.get(pid).keySet()) {
                Long occur = map.get(pid).get(resolution);
                if (resolution == 0) {
                    result.add(String.format("%d\t%d\t%s\t%s", occur * duration, 0, "audio", pid));
                } else {
                    result.add(String.format("%d\t%d\t%s\t%s", occur * duration, resolution, "single_resolution", pid));
                    totalResolution += resolution * occur;
                }
            }
        }
        if (totalResolution > 0) {
            result.add(String.format("%d\t%d\t%s\t%s", duration, totalResolution, "total_resolution", String.join(",", map.keySet())));
        }
        return result;
    }
}
