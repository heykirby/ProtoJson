package com.kuaishou.data.udf.video;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * @author <zhouwenjia@kuaishou.com>
 * Created on 2022-01-13
 */
public class ResolutionExtract extends UDF {
    public String evaluate(String paramJson,String type){
        String str=null;
        try {
            Map parse = (Map) new JSONParser().parse(paramJson);
            str=parse.get("outputInfo").toString();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Map paraJsonMap=null;
        try {
            paraJsonMap = (Map) new JSONParser().parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //取出分辨率  不确定width height具体类型的情况
        int width = Integer.parseInt(String.valueOf(paraJsonMap.get("width")));
        int height = Integer.parseInt(String.valueOf(paraJsonMap.get("height")));
        int minLen=Math.min( width,height);

        String resolution = getResolution(minLen);
        String codecName = (String) paraJsonMap.get("codecName");
        if(type.equals("Transcode")){
            return codecName+"."+resolution;
        }else if(type.equals("SDR2HDR")){
            return resolution;
        }
        return "";
    }
    public static String getResolution(int minLen){
        if(minLen>1440){
            return "4K";
        }else if(minLen>1080 && minLen<=1440){
            return "2K";
        }else if(minLen>720 && minLen<=1080){
            return "1080P";
        }else if(minLen>540 && minLen<=720){
            return "720P";
        }else if(minLen>480 && minLen<=540){
            return "480P";
        }else if(minLen<=360){
            return "360P";
        }
        return "";
    }
}
