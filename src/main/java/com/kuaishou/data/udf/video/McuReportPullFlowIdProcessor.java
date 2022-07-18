package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.format.DateTimeFormat;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-05-06
 */
public class McuReportPullFlowIdProcessor extends UDF {
    public static ArrayList<String> evaluate(String json, String ltStr, String ctStr) throws IOException, IOException {
        long lt = (ltStr != null && !ltStr.isEmpty()) ? Long.parseLong(ltStr) : -1;
        long ct = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").parseDateTime(ctStr).getMillis();
        ArrayList<String> result = new ArrayList<>();
        long realDuration = (lt == -1 || lt < ct) ? 1000 : (lt - ct);
        if (StringUtils.isEmpty(json)) {
            result.add(String.format("%d\t0\t%s\t0", realDuration, "single_audio"));
            result.add(String.format("%d\t0\t%s\t0", realDuration, "audio"));
            return result;
        }
        JsonNode downDetailsNode = JsonUtils.parse(json);
        TreeMap<String, HashMap<Long, Long>> uidResolutionMap = new TreeMap<>();
        if (downDetailsNode.has("v") && downDetailsNode.path("v").size() > 0) {
            Iterator<Entry<String, JsonNode>> iterator = downDetailsNode.path("v").fields();
            while (iterator.hasNext()) {
                JsonNode single = iterator.next().getValue();
                String uid = single.path("uid").asText();
                if (!uidResolutionMap.containsKey(uid)) {
                    uidResolutionMap.put(uid, new HashMap<>());
                }
                long resolution = single.path("width").asLong(0) * single.path("height").asLong(0);
                uidResolutionMap.get(uid).put(resolution, uidResolutionMap.get(uid).getOrDefault(resolution, 0l) + 1);
            }
            if (downDetailsNode.has("a") && downDetailsNode.path("a").size() > 0) {
                Iterator<Entry<String, JsonNode>> aIterator = downDetailsNode.path("a").fields();
                while (aIterator.hasNext()) {
                    JsonNode aSingle = aIterator.next().getValue();
                    String uid = aSingle.path("uid").asText();
                    // 分辨率map不包含uid，表示没有视频用量｜或者前面已经计算了音频用量
                    if (uid != null && !uidResolutionMap.containsKey(uid)) {
                        uidResolutionMap.put(uid, new HashMap<>());
                        uidResolutionMap.get(uid).put(0L, 1L);
                        result.add(String.format("%d\t0\t%s\t0", realDuration, "single_audio"));
                    }
                }
            }
            long totalResolution = 0L;
            for (String uid : uidResolutionMap.keySet()) {
                HashMap<Long, Long> map = uidResolutionMap.get(uid);
                if (map.size() == 1 && map.containsKey(0L)) {
                    result.add(String.format("%d\t0\t%s\t0", realDuration, "single_audio"));
                    continue;
                }
                for (long re : map.keySet()) {
                    totalResolution += map.get(re) * realDuration;
                    result.add(
                            String.format("%d\t%d\t%s\t%s", map.get(re) * realDuration, re, "single_resolution", uid));
                }
            }
            result.add(String.format("%d\t%d\t%s\t%s", realDuration, totalResolution, "total_resolution",
                    String.join(",", uidResolutionMap.keySet())));
        } else {
            result.add(String.format("%d\t0\t%s\t0", realDuration, "single_audio"));
            result.add(String.format("%d\t0\t%s\t0", realDuration, "audio"));
        }
        return result;
    }
}
