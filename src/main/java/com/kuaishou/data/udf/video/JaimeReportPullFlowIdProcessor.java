package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-05-18
 */
public class JaimeReportPullFlowIdProcessor extends UDF {
    public static ArrayList<String> evaluate(String json) throws IOException {
        JsonNode root = JsonUtils.parse(json);
        Iterator<Entry<String, JsonNode>> fields = root.fields();
        ArrayList<String> result = new ArrayList<>();
        // 视频和音频 <pid,<resolution, duration>>
        TreeMap<String, HashMap<Long, Long>> pidMap = new TreeMap<>();
        Set<String> videoPidSet = new HashSet<>();
        long duration = root.path("duration").asLong(0);
        String mpuTp = root.path("mpu_tp").asText();
        boolean isRecord = false;
        int sourceType = -1;

        while (fields.hasNext()) {
            Entry<String, JsonNode> entry = fields.next();
            if (Pattern.matches("^rtmp\\d+$", entry.getKey())
                    && entry.getValue().path("url_type").asText().equals("RECORD_URL")) {
                isRecord = true;
                continue;
            }
            if (Pattern.matches("^p\\d+$", entry.getKey())) {
                String pid = entry.getValue().path("pid").asText("UNKNOWN");
                Long height = entry.getValue().path("v_h").asLong(0L);
                Long width = entry.getValue().path("v_w").asLong(0L);
                long resolution = height * width;
                if (resolution > 0) {
                    videoPidSet.add(pid);
                }
                if (!pidMap.containsKey(pid)) {
                    pidMap.put(pid, new HashMap<>());
                }
                pidMap.get(pid).put(resolution, pidMap.get(pid).getOrDefault(resolution, 0L) + 1);
            }
        }
        sourceType = "audit".equals(mpuTp) ? 3 : (isRecord ? 4 : 2);
        if (!videoPidSet.isEmpty()) {
            long totalResolution = 0L;
            for (String pid : pidMap.keySet()) {
                HashMap<Long, Long> map = pidMap.get(pid);
                if (map.size() == 1 && map.containsKey(0L)) {
                    result.add(String.format("%d\t%d\t%s\t%s\t0", duration, 0, "single_audio", sourceType));
                    continue;
                }
                for (long re : map.keySet()) {
                    if (re == 0L) {
                        continue;
                    }
                    totalResolution += re * map.get(re);
                    result.add(String.format("%d\t%d\t%s\t%d\t%s", map.get(re) * duration, re, "single_resolution",
                            sourceType, pid));
                }
            }
            result.add(String.format("%d\t%d\t%s\t%s\t%s", duration, totalResolution, "total_resolution", sourceType,
                    String.join(",", pidMap.keySet())));
        } else {
            result.add(String.format("%d\t%d\t%s\t%s\t0", duration, 0, "single_audio", sourceType));
            result.add(String.format("%d\t%d\t%s\t%s\t0", duration, 0, "audio", sourceType));
        }
        return result;
    }
}
