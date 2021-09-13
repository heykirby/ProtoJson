package com.kuaishou.data.udf.video;

import com.kuaishou.framework.config.util.CityHashTailNumberConfig;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Objects;

public class ObjectHashUDF extends UDF {
    public ObjectHashUDF() {}

    public boolean evaluate(String deviceId) {
        return evaluate(deviceId, "100;0-14");
    }

    public boolean evaluate(String deviceId, String configString) {
        return CityHashTailNumberConfig.from(configString).isOnFor(Objects.hash(deviceId));
    }

    public static void main(String[] args) {
        ObjectHashUDF udf = new ObjectHashUDF();
        System.out.println(udf.evaluate("D7627BEE-ACAC-475E-89F5-DACF18257232", "100;0-14"));
    }
}