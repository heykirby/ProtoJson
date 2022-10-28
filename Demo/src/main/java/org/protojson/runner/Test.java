package org.protojson.runner;

import java.io.IOException;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-10-27
 */
public class Test {
    public static void main(String[] args) throws IOException {
        String json = "{\"a\":1,\"b\":{\"c\":\"xx\",\"d\":[1,2,3],\"e\":[[1,2,3]]}}";
        JsonRowConverter converter = new JsonRowConverter("a", "b", "b.c", "b.d", "b.d.1", "b.e", "b.e.0", "b.e.0.1");

        System.out.println(String.join("," ,converter.process(json)));
    }
}
