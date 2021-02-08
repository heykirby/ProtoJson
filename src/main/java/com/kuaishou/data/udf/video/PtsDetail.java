package com.kuaishou.data.udf.video;

import java.util.ArrayList;
import java.util.Base64;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.Text;

import com.google.common.collect.ImmutableList;

/**
 * @author xiazehui <xiazehui@kuaishou.com>
 * Created on 2019-09-04
 */
public class PtsDetail extends UDF {


    public ArrayList<ImmutableList<Long>> evaluate(Text pts, Text timesatmp) throws UDFArgumentException {

        if (pts == null || timesatmp == null) {
            throw new UDFArgumentException("must take two arguments");
        }

        String str = pts.toString();
        Long base = Long.valueOf(timesatmp.toString());
        ArrayList<Long> ptsList = new ArrayList<>();
        ArrayList<ImmutableList<Long>> ptsDetail = new ArrayList<>();
        if (str.contains(",")) {
            for (String s : str.split(",")) {
                try {
                    ptsList.add(Long.parseLong(s));
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        } else {
            byte[] bytes = null;
            try {
                bytes = Base64.getDecoder().decode(str);
            } catch (IllegalArgumentException e) {
                return null;
            }
            long s = 0;
            for (int i = 0; i < bytes.length; i += 2) {
                s += (bytes[i] << 8) | (bytes[i + 1] & 0xff);
                ptsList.add(s);
            }
        }
        for (int i = 2; i < ptsList.size(); i += 1) {
            if (ptsList.get(i) - ptsList.get(i - 2) > base) {
                ptsDetail.add(ImmutableList.of(ptsList.get(i - 2) , ptsList.get(i)));
            }
        }
        return ptsDetail;
    }
}
