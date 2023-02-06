package com.kuaishou.data.udf.video;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.protojson.runner.JsonRowConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-09-27
 */
public class KsonTuple extends GenericUDTF {
    private static final Logger LOG = LoggerFactory.getLogger(KsonTuple.class.getName());
    private boolean isParsed;
    private int numCols;
    private transient ObjectInspector[] inputOIs;
    private transient Text[] retCols;
    private transient Text[] cols;
    private transient Object[] nullCols;
    private String[] converterParams;
    private transient JsonRowConverter converter;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        isParsed = false;
        inputOIs = argOIs;
        numCols = argOIs.length - 1;
        if (numCols < 1) {
            throw new UDFArgumentException("json_tuple() takes at least two arguments: " +
                    "the json string and a path expression");
        }
        cols = new Text[numCols];
        retCols = new Text[numCols];
        nullCols = new Object[numCols];
        converterParams = new String[numCols];
        for (int i = 0; i < argOIs.length; ++i) {
            if (argOIs[i].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                    !argOIs[i].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
                throw new UDFArgumentException("json_tuple()'s arguments have to be string type");
            }
        }
        for (int i = 0; i < numCols; ++i) {
            cols[i] = new Text();
            retCols[i] = cols[i];
            nullCols[i] = null;
        }
        // construct output object inspector
        ArrayList<String> fieldNames = new ArrayList<String>(numCols);
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(numCols);
        for (int i = 0; i < numCols; ++i) {
            fieldNames.add("c" + i);
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if (args[0] == null) {
            forward(nullCols);
            return;
        }
        if (!isParsed) {
            for (int i = 0; i < numCols; i++) {
                converterParams[i] = ((StringObjectInspector) inputOIs[i + 1]).getPrimitiveJavaObject(args[i + 1]);
            }
            converter = new JsonRowConverter(converterParams);
            isParsed = true;
        }
        try {
            String[] resultStrs =
                    converter.process(((StringObjectInspector) inputOIs[0]).getPrimitiveJavaObject(args[0]));
            for (int i = 0; i < numCols; i++) {
                if (resultStrs[i] == null) {
                    retCols[i] = null;
                } else {
                    if (retCols[i] == null) {
                        retCols[i] = cols[i];
                    }
                    retCols[i].set(resultStrs[i]);
                }
            }
            forward(retCols);
        } catch (Exception e) {
            LOG.error("JSON parsing/evaluation exception" + e);
            forward(nullCols);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
