package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author <zhouwenjia@kuaishou.com>
 * Created on 2022-06-13
 */
public class JaimeReportMixProcessor extends UDF {
    public static ArrayList<String> evaluate(String json) throws IOException {
        JsonNode root = JsonUtils.parse(json);
        Iterator<Entry<String, JsonNode>> fields = root.fields();
        ArrayList<String> result = new ArrayList<>();
        long duration = root.path("duration").asLong(0L);
        long vWight = root.path("v_width").asLong(0L);
        long vHeight = root.path("v_height").asLong(0L);
        String mpuTp = root.path("mpu_tp").asText();
        String sourceType = "mix";

        while (fields.hasNext()) {
            Entry<String, JsonNode> entry = fields.next();
            //rtmp_stat列表中存在URL_TYPE为RECORD_URL的时候
            if (Pattern.matches("^rtmp\\d+$", entry.getKey())
                    && "RECORD_URL".equals(entry.getValue().path("url_type").asText())) {
                sourceType = "record";
            }
        }
        // mpu_tp为audit时，值为audit
        if ("audit".equals(mpuTp)) {
            sourceType = "audit";
        }
        long resolution = vWight * vHeight;
        String type = resolution > 0 ? "single_resolution" : "audio";
        result.add(String.format("%d\t%d\t%s\t%s", duration, resolution, type, sourceType));
        if ("record".equals(sourceType)) {
            result.add(String.format("%d\t%d\t%s\t%s", duration, resolution, type, "mix"));
        }

        // 过滤audit空流
        if ("audit".equals(mpuTp)) {
            if (resolution <= 0 && root.path("rtmp1").path("a_bytes").asLong(0L) <= 0) {
                return new ArrayList<>();
            }
        }
        return result;
    }
}
