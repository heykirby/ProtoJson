package com.kuaishou.data.udf.video;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-07-14
 */
public class ImageResolutionParse extends UDF{
    public static String evaluate(String str) {
        try {
            if (StringUtils.isEmpty(str)) {
                return "other";
            }
            String[] xes = str.split("x");
            long reso = Math.max(Long.parseLong(xes[0]), Long.parseLong(xes[1]));
            if (reso < 0) {
                return "other";
            } else if (reso < 360) {
                return "<360P";
            } else if (reso < 480) {
                return "360P";
            } else if (reso < 540) {
                return "480P";
            } else if (reso < 576) {
                return "540P";
            } else if (reso < 720) {
                return "576P";
            } else if (reso < 1080) {
                return "720P";
            } else if (reso < 2160) {
                return "1080P";
            } else if (reso < 4960) {
                return "2K";
            } else {
                return "4K";
            }
        } catch (Exception e) {
            return "other";
        }

    }
}
