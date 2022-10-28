package org.protojson.runner;

import org.protojson.pojo.JsonNode;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-10-27
 */
public class Test1 {
    static JsonNode n1 = new JsonNode("a");
    static JsonNode n2 = new JsonNode("b");
    public static void main(String[] args) {
        JsonNode node = n1;
        System.out.println(node.getName());
        test(node);
        System.out.println(node.getName());
    }
    private static void test(JsonNode node) {
        node = n2;
    }
}
