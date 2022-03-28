package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-01-14
 */
public class VideoToBExtractResolution extends UDF {
    public static String evaluate(String paramJson, String type) {
        try {
            JsonNode node = JsonUtils.parse(paramJson);
            if (!node.has("outputInfo")) {
                return "UNKNOWN";
            }
            JsonNode outputInfoNode = node.path("outputInfo");
            String codecName = outputInfoNode.path("videoCodec").asText("UNKNOWN");
            double fps = outputInfoNode.path("fps").asDouble(0);
            long width = outputInfoNode.path("width").asLong(0);
            long height = outputInfoNode.path("height").asLong(0);
            String resolution = getResolution(Math.min(width, height));
            if ("Transcode".equals(type)) {
                if (!codecName.equalsIgnoreCase("H264")
                        && !codecName.equalsIgnoreCase("H265")) {
                    return null;
                }
                return (codecName + "." + resolution).toUpperCase();
            } else if ("SDR2HDR".equals(type)) {
                return resolution.toUpperCase();
            } else if ("SR".equals(type)) {
                if (!codecName.equalsIgnoreCase("H264")
                        && !codecName.equalsIgnoreCase("H265")) {
                    return null;
                }
                String str = "";
                if (fps < 30) str = "30FPS";
                else str = "60FPS";
                return (codecName + "." + resolution + "." + str).toUpperCase();
            }
            return "UNKNOWN";
        } catch (Exception e) {
            return "UNKNOWN";
        }


    }

    public static String getResolution(long minLen) {
        if (minLen <= 360) {
            return "360P";
        } else if (minLen <= 480) {
            return "480P";
        } else if (minLen <= 540) {
            return "540P";
        } else if (minLen <= 720) {
            return "720P";
        } else if (minLen <= 1080) {
            return "1080P";
        } else if (minLen <= 1440) {
            return "2K";
        } else {
            return "4K";
        }
    }
}
