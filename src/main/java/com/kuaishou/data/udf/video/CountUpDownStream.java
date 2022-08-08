package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-08-01
 */
public class CountUpDownStream extends UDF {
    public static ArrayList<String> evaluate(String json) throws IOException {
        ArrayList<String> result = new ArrayList<>();
        if (json == null || json.isEmpty()) {
            return result;
        }
        JsonNode root = JsonUtils.parse(json);
        if (root.has("up")) {
            long v = root.path("up").path("v_bytes").asLong();
            long a = root.path("up").path("a_bytes").asLong();
            long d = root.path("up").path("d_bytes").asLong();
            result.add(String.format("up_v_%d", v));
            result.add(String.format("up_a_%d", a));
            result.add(String.format("up_d_%d", d));
        }
        if (root.has("down")) {
            JsonNode down = root.path("down");
            if (down.has("v")) {
                long v = 0;
                for (JsonNode node : down.path("v")) {
                    v += node.path("bytes").asLong();
                }
                result.add(String.format("down_v_%d", v));
            }
            if (down.has("a")) {
                long a = 0;
                for (JsonNode node : down.path("a")) {
                    a += node.path("bytes").asLong();
                }
                result.add(String.format("down_a_%d", a));
            }
            if (down.has("d")) {
                long d = 0;
                for (JsonNode node : down.path("d")) {
                    d += node.path("bytes").asLong();
                }
                result.add(String.format("down_d_%d", d));
            }
        }
        return result;
    }
}
