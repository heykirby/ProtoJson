package com.kuaishou.data.udf.video;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import eu.bitwalker.useragentutils.UserAgent;

public class SplitUserAgent extends GenericUDTF {


    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
        }
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
        }
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("system");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("browser");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("version");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }
    @Override
    public void process(Object[] arg){
        try {
            if(arg == null || arg.length == 0)
                return;
            String input = arg[0].toString();
            String result[] = ua_parse(input).split("\t");
            forward(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws HiveException {

    }
    public String ua_parse(String userAgent){
        StringBuilder builder = new StringBuilder();
        UserAgent ua = new UserAgent(userAgent.toString());
        builder.append(ua.getOperatingSystem()+"\t"+ua.getBrowser()+"\t"+ua.getBrowserVersion());
        return builder.toString();
    }

}
