package com.kuaishou.data.udf.video;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

/**
 * @author xiazehui <xiazehui@kuaishou.com>
 * Created on 2019-09-04
 */
public class ParsePTS extends UDF {


    public Integer evaluate(String str, Integer base) throws UDFArgumentException {

        if (str == null || str.length() == 0 || base == null) {
            throw new UDFArgumentException("must take two arguments");
        }

        try {
            if (str.contains(",")) {
                return -1;
            }
            byte[] decode = Base64.decodeBase64(str);
            int i = 6;
            while (i < decode.length) {
                int cur = decode[i] < 0 ? decode[i] + 256 : decode[i];
                int cur1 = decode[i + 1] < 0 ? decode[i + 1] + 256 : decode[i + 1];
                int cur_4 = decode[i - 4] < 0 ? decode[i - 4] + 256 : decode[i - 4];
                int cur_3 = decode[i - 3] < 0 ? decode[i - 3] + 256 : decode[i - 3];
                int pts_1 = cur << 8 | cur1;
                int pts_2 = cur_4 << 8 | cur_3;
                int pts_diff = pts_2 - pts_1;
                if (pts_diff >= base) {
                    return 1;
                }
                i += 2;
            }
            return 0;
        } catch (Exception e) {
            return -1;
        }
    }
}
