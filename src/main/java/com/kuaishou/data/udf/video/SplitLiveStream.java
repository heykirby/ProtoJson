package com.kuaishou.data.udf.video;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.shaded.org.apache.commons.io.FilenameUtils;

@Description(name = "SplitLiveStream",
        value = "_FUNC_(url) - parse live play url,rerurn stream id and transcode template.",
        extended = "The value is returned as an array(string).none template return  [\"stream_id\","
                + "\"0\"] ,error url rerurn  [\"-1\",\"-1\"]\n"
                + "Example:\n"
                + "  > SELECT _FUNC_(url); return [\"stream_id\",\"transcode_template\"]")
public class SplitLiveStream extends UDF {

    public ArrayList<String> evaluate(Text input) {
        if (input == null || input.getLength() < 1) {
            return null;
        }

        String url = input.toString();
        ArrayList<String> contacts = new ArrayList<String>();
        try {
            URI uri = URI.create(url);
            String path = uri.getPath();
            String name = FilenameUtils.getBaseName(path);
            String[] split = StringUtils.split(name, "_");
            if (split.length == 1) {
                System.out.println(name);
                contacts.add(name);
                contacts.add("0");
            } else if (split.length == 2 && split[0].length() >= 11) {
                contacts.add(split[0]);
                contacts.add(split[1]);
            } else if (split.length == 2 && split[0].length() < 11) {
                contacts.add(name);
                contacts.add("0");
            } else if (split.length > 2) {
                String[] newSplit = Arrays.copyOf(split, split.length - 1);
                String tmpStream = StringUtils.join(newSplit, "_");
                if (tmpStream.length() >= 11) {
                    contacts.add(tmpStream);
                    contacts.add(split[split.length - 1]);
                } else {
                    contacts.add(name);
                    contacts.add("0");
                }

            }
        } catch (Exception e) {
            contacts.add("-1");
            contacts.add("-1");
            return contacts;
        }
        return contacts;

    }
    //"sid":"PJaEXnBd0xQ_hd2000","osid":"PJaEXnBd0xQ",
    public String evaluate(Text osid,Text sid) {
        if (sid == null || sid.getLength() < 1 || osid == null || osid.getLength() < 1) {
            return "null";
        }
        String sidStr = sid.toString();
        String osidStr = osid.toString();
        if (sid.getLength()==osid.getLength()){
            return "null";
        }else {
            return sidStr.replace(osidStr,"").replace("_","");
        }
    }
}
