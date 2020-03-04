package com.kuaishou.data.udf.video;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import com.kuaishou.photo.util.PhotoCdnUtils;

/**
 * @author xiazehui <xiazehui@kuaishou.com>
 * Created on 2020-02-13
 */
public class ParsePhotoId extends UDF {
    public long evaluate(String urlStr) throws UDFArgumentException {

        if (urlStr == null || urlStr.length() == 0) {
            return 0;
        }
        try {
           return PhotoCdnUtils.parsePhotoId(urlStr);
        }catch (Exception e){
            return 0;
        }

    }
}
