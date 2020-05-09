package com.kuaishou.data.udf.video;

import static org.apache.commons.codec.binary.Base64.decodeBase64;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.math.NumberUtils.toLong;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

//import com.kuaishou.photo.util.PhotoCdnUtils;

/**
 * @author xiazehui <xiazehui@kuaishou.com>
 * Created on 2020-02-13
 */
public class ParsePhotoId extends UDF {
    public long evaluate(String urlStr) throws UDFArgumentException {

        if (urlStr == null || urlStr.length() == 0) {
            return 0;
        }
        // /upic/2015/10/13/01/BMjAxNTEwMTMwMTAwMjFfMzA3OTI3NDhfNDE0MTE4OTU4XzJfMw==.mp4
        // 20130125171134_218457_photoId_1_4
        if (isNotBlank(urlStr)) {
            int from = urlStr.lastIndexOf('/');
            int to = urlStr.indexOf('.', from);
            if (from > 0 && to > 0 && from + 2 < to) {
                try {
                    return toLong(
                            (new String(decodeBase64(urlStr.substring(from + 2, to)))).split("_")[2]);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        return 0;
        //           return PhotoCdnUtils.parsePhotoId(urlStr);


    }
}
