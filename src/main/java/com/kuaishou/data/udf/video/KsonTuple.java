package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-09-27
 */
public class KsonTuple extends GenericUDTF {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private transient Text[] retCols;
    private transient Object[] nullCols;
    static {

    }
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        ObjectInspector[] inputOIs = argOIs;
        int numCols = argOIs.length - 1;
        return null;
    }

    @Override
    public void process(Object[] args) throws HiveException {

    }

    @Override
    public void close() throws HiveException {

    }
}
