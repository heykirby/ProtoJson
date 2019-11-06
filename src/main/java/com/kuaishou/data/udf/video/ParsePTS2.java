package com.kuaishou.data.udf.video;

import java.util.ArrayList;
import java.util.Base64;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;


public class ParsePTS2 extends UDF {


    public Integer evaluate(String str, Integer base) throws UDFArgumentException {

        if (str == null || str.length() == 0 || base == null) {
            throw new UDFArgumentException("must take two arguments");
        }

        ArrayList<Long> ptsList = new ArrayList<>();
        if (str.contains(",")) {
            for (String s : str.split(",")) {
                try {
                    ptsList.add(Long.parseLong(s));
                } catch (NumberFormatException e) {
                    return 0;
                }
            }
        } else {
            byte[] bytes = null;
            try {
                bytes = Base64.getDecoder().decode(str);
            } catch (IllegalArgumentException e) {
                return 0;
            }
            long s = 0;
            for (int i = 0; i < bytes.length; i += 2) {
                s += (bytes[i] << 8) | (bytes[i + 1] & 0xff);
                ptsList.add(s);
            }
        }
        int times = 0;
        for (int i = 2; i < ptsList.size(); i += 1) {
            if (ptsList.get(i) - ptsList.get(i - 2) > base) {
                times = times + 1;
            }
        }
        return times;
    }
}
