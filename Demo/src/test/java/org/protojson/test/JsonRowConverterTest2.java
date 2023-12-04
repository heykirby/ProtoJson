package org.protojson.test;

import java.io.IOException;

import org.junit.Test;
import org.protojson.runner.JsonRowConverter;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2023-12-04
 */
public class JsonRowConverterTest2 {
    @Test
    public void test() throws IOException {
        String str1 = "{\"a\":{\"a1\":[{\"a2\":\"xxxx\",\"a3\":123,\"a4\":[1,2,3,4]},{\"a5\":\"ffff\",\"a6\":1234,"
                + "\"a7\":[7,9]}],\"a8\":[1,2,3,4],\"a9\":123},\"b\":[1,2,3,4],\"c\":null,\"d\":\"xxx\","
                + "\"e\":{\"e1\":{\"e2\":123,\"e3\":[{},{\"e4\":[4,5,7]}]}}}";
        String str2 = "{\"b\":[1,2,3,4],\"c\":null,\"d\":\"xxx\","
                + "\"e\":{\"e1\":{\"e2\":123,\"e3\":[{},{\"e4\":[4,5,7]}]}}}";
        JsonRowConverter converter = new JsonRowConverter("a", "a.a1", "a.a1.a2", "a.a1.0", "a.a1.0.a2", "a.a1.0.a4.3",
                "a.a1.1.a7", "a.a8", "b", "c", "e.e1.e3.0");
        System.out.println(String.join("\n", converter.process(str1)));
        System.out.println("+++");
        System.out.println(String.join("\n", converter.process(str2)));
    }
}
