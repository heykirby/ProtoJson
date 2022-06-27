package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.format.DateTimeFormat;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-03-31
 */
public class ParseDate2Timestamp extends UDF {
    public static long evaluate(String date, String format) {
        return DateTimeFormat.forPattern(format).parseDateTime(date).getMillis();
    }
}
