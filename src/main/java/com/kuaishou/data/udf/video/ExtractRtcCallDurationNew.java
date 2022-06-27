package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.format.DateTimeFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;
import org.springframework.util.StringUtils;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-04-27
 */
public class ExtractRtcCallDurationNew extends UDF {
    public static ArrayList<String> evaluate(String json, String ltStr, String ctStr) throws IOException {
        long lt = (ltStr != null) ? Long.parseLong(ltStr) : -1;
        long ct = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS+08:00").parseDateTime(ctStr).getMillis();
        ArrayList<String> result = new ArrayList<>();
        long realDuration = (lt == -1 || lt < ct) ? 1000 : (lt - ct);
        if (StringUtils.isEmpty(json)) {
            result.add(String.format("%d\t0\t%s", realDuration, "single_audio"));
            result.add(String.format("%d\t0\t%s", realDuration, "audio"));
            return result;
        }
        JsonNode downDetailsNode = JsonUtils.parse(json);
        Set<String> videoPidSet = new HashSet<>();
        if (downDetailsNode.has("v") && downDetailsNode.path("v").size() > 0) {
            Iterator<Entry<String, JsonNode>> iterator = downDetailsNode.path("v").fields();
            HashMap<Long, Integer> resolutionMap = new HashMap<>();
            while (iterator.hasNext()) {
                JsonNode single = iterator.next().getValue();
                videoPidSet.add(single.path("pid").asText());
                long resolution = single.path("width").asLong(0) * single.path("height").asLong(0);
                if (resolutionMap.containsKey(resolution)) {
                    resolutionMap.put(resolution, resolutionMap.get(resolution) + 1);
                } else {
                    resolutionMap.put(resolution, 1);
                }
            }
            if (resolutionMap.size() == 0) {
                return result;
            }
            int totalResolution = 0;
            for (long resolution : resolutionMap.keySet()) {
                result.add(String.format("%d\t%d\t%s", resolutionMap.get(resolution) * realDuration, resolution, "single_resolution"));
                totalResolution += resolution * resolutionMap.get(resolution);
            }
            result.add(String.format("%d\t%d\t%s", realDuration, totalResolution, "total_resolution"));
            if (downDetailsNode.has("a") && downDetailsNode.path("a").size() > 0) {
                Iterator<Entry<String, JsonNode>> aIterator = downDetailsNode.path("a").fields();
                while (aIterator.hasNext()) {
                    JsonNode aSingle = aIterator.next().getValue();
                    String pid = aSingle.path("pid").asText();
                    if (pid != null && !videoPidSet.contains(pid)) {
                        result.add(String.format("%d\t0\t%s", realDuration, "single_audio"));
                    }
                }
            }
        } else {
            result.add(String.format("%d\t0\t%s", realDuration, "single_audio"));
            result.add(String.format("%d\t0\t%s", realDuration, "audio"));
        }
        return result;
    }
}
