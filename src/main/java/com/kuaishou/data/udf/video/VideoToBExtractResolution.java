package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-01-14
 */
public class VideoToBExtractResolution extends UDF {
    public static String execute(String paramJson,String type) {
        try {
            JsonNode node = JsonUtils.parse(paramJson);
            if (!node.has("outputInfo")) return "UNKNOWN";
            JsonNode outputInfoNode = node.path("outputInfo");
            String codecName = outputInfoNode.path("codecName").asText("UNKNOWN");
            long width = outputInfoNode.path("width").asLong(0);
            long height = outputInfoNode.path("height").asLong(0);
            String resolution = getResolution(Math.min(width, height));
            if("Transcode".equals(type)){
                return codecName+"."+resolution;
            }else if("SDR2HDR".equals(type)){
                return resolution;
            }
            return "UNKNOWN";
        } catch (Exception e) {
            return "UNKNOWN";
        }


    }
    public static String getResolution(long minLen){
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
