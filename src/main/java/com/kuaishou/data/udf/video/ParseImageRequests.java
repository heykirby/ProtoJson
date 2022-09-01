package com.kuaishou.data.udf.video;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-08-30
 */
public class ParseImageRequests extends GenericUDTF {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("status");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("error_code");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fieldNames.add("error_message");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("url");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("http_code");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fieldNames.add("server_ip");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("protocol");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("received_bytes");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fieldNames.add("cost");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fieldNames.add("dns_cost");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fieldNames.add("connect_cost");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fieldNames.add("waiting_response_cost");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fieldNames.add("response_cost");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

//    @Override
//    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
//        ArrayList<String> fieldNames = new ArrayList<>();
//        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
//        fieldNames.add("status");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//        fieldNames.add("error_code");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
//        fieldNames.add("error_message");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//        fieldNames.add("url");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//        fieldNames.add("http_code");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
//        fieldNames.add("server_ip");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//        fieldNames.add("protocol");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//        fieldNames.add("received_bytes");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
//        fieldNames.add("cost");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
//        fieldNames.add("dns_cost");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
//        fieldNames.add("connect_cost");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
//        fieldNames.add("waiting_response_cost");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
//        fieldNames.add("response_cost");
//        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
//        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
//    }

    @Override
    public void process(Object[] args) throws HiveException {
        try {
            String input = args[0].toString();
            ArrayNode root = (ArrayNode) MAPPER.readTree(input);
            for (JsonNode node : root) {
                Object[] result = new Object[13];
                result[0] = node.path("status").asText();
                result[1] = node.path("error_code").asLong(-1);
                result[2] = node.path("error_message").asText();
                result[3] = node.path("url").asText();
                result[4] = node.path("http_code").asLong(-1);
                result[5] = node.path("server_ip").asText();
                result[6] = node.path("protocol").asText();
                result[7] = node.path("received_bytes").asLong(-1);
                result[8] = node.path("cost").asLong(-1);
                result[9] = node.path("dns_cost").asLong(-1);
                result[10] = node.path("connect_cost").asLong(-1);
                result[11] = node.path("waiting_response_cost").asLong(-1);
                result[12] = node.path("response_cost").asLong(-1);
                forward(result);
            }
        } catch (Exception e) {
            return;
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
