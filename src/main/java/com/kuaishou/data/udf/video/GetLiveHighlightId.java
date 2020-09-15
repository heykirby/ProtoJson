package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.livestream.util.LiveUuidUtils;

/**
 * @author wuxuexin <wuxuexin@kuaishou.com>
 * Created on 2020-09-15
 */
public class GetLiveHighlightId extends UDF {

    public long evaluate(String highlightUuid) {
        long id = 0L;
        try {
            id = LiveUuidUtils.getLiveHighlightLongId(highlightUuid);
        } catch (Exception ignored) {
        }

        return id;

    }

    public String evaluate(long highlightId) {
        String s = "";
        try {
            s = LiveUuidUtils.getLiveHighlightUuid(highlightId);
        } catch (Exception ignored) {
        }

        return s;

    }

}
