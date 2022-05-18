package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.format.DateTimeFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-05-06
 */
public class ExtractRtcCallDurationFlowIdNew extends UDF {
    public static ArrayList<String> evaluate(String json, String ltStr, String ctStr) throws IOException, IOException {
        long lt = (ltStr != null) ? Long.parseLong(ltStr) : -1;
        long ct = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").parseDateTime(ctStr).getMillis();
        ArrayList<String> result = new ArrayList<>();
        long realDuration = (lt == -1 || lt < ct) ? 1000 : (lt - ct);
        if (json == null) {
            result.add(String.format("%d\t0\t%s\t0", realDuration, "audio"));
            return result;
        }
        JsonNode downDetailsNode = JsonUtils.parse(json);
        if (downDetailsNode.has("v") && downDetailsNode.path("v").size() > 0) {
            Iterator<Entry<String, JsonNode>> iterator = downDetailsNode.path("v").fields();
            TreeMap<String, HashMap<Long, Long>> map = new TreeMap<>();
            while (iterator.hasNext()) {
                JsonNode single = iterator.next().getValue();
                long resolution = single.path("width").asLong(0) * single.path("height").asLong(0);
                String flowId = single.path("sid").asText("unknown");
                if (!map.containsKey(flowId)) {
                    map.put(flowId, new HashMap<>());
                }
                map.get(flowId).put(resolution, map.get(flowId).getOrDefault(resolution, 0l) + 1);
            }
            if (map.size() == 0) {
                return result;
            }
            int totalResolution = 0;
            for (String flowId : map.keySet()) {
                for (long resolution : map.get(flowId).keySet()) {
                    Long occur = map.get(flowId).get(resolution);
                    result.add(String.format("%d\t%d\t%s\t%s", occur * realDuration, resolution, "single_resolution", flowId));
                    totalResolution += resolution * occur;
                }
            }
            result.add(String.format("%d\t%d\t%s\t%s", realDuration, totalResolution, "total_resolution", String.join(",", map.keySet())));
        } else {
            result.add(String.format("%d\t0\t%s\t0", realDuration, "audio"));
        }
        return result;
    }
}
