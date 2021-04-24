package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kuaishou.data.udf.video.utils.JsonUtils;

/**
 * @author yehe <yehe@kuaishou.com>
 * Created on 2021-04-24
 */
public class ExtractTailNoFromCDN extends UDF{
    public static String evaluate(String params) throws JsonProcessingException {
        HashMap<String, List> hm = new HashMap<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(params);
            Iterator<String> keys = jsonNode.fieldNames();
            while (keys.hasNext()) {
                String key = keys.next();
                String value = jsonNode.get(key).asText("").split(";", -1)[1];
                List<Integer> tailNosList = new ArrayList<>();
                Stream.of(value.split(",")).forEach(str -> {
                    if ("".equals(str)) {
                        hm.put(key, tailNosList);
                        return;
                    }
                    String[] rangeNos = str.split("-", 0);
                    if (rangeNos.length > 1) {
                        List<Integer> list =
                                IntStream.rangeClosed(Integer.valueOf(rangeNos[0]), Integer.valueOf(rangeNos[1]))
                                        .boxed().collect(
                                        Collectors.toList());
                        tailNosList.addAll(list);
                    } else {
                        tailNosList.add(Integer.valueOf(rangeNos[0]));
                    }
                });
                hm.put(key, tailNosList);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (JsonUtils.getMapper().writeValueAsString(hm));
    }
}
