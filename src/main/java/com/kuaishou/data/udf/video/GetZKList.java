package com.kuaishou.data.udf.video;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.kuaishou.data.udf.video.utils.ZkUtils;


/**
 * @author xiazehui <xiazehui@kuaishou.com>
 * Created on 2020-04-27
 */
public class GetZKList extends GenericUDTF {

    private static Set zkValue;
    private PrimitiveObjectInspector stringOI;
    public Set evaluate(String path) throws Exception {
        if (path == null || path.equals("")) {
            return null;
        }
        if (zkValue == null) {
            zkValue = ZkUtils.getValue(path);
        }
        return zkValue;
    }

    public boolean evaluate(String path, String input) throws Exception {
        if (path == null || path.equals("") || input == null || input.equals("")) {
            return false;
        }
        return evaluate(path).contains(input);
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("NameParserGenericUDTF() takes exactly one argument");
        }
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a parameter");
        }

        // 输入格式（inspectors）
        stringOI = (PrimitiveObjectInspector) args[0];
        // 输出格式（inspectors）
        List<String> fieldNames = new ArrayList<String>(1);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(1);
        fieldNames.add("name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    @Override
    public void process(Object[] objects) throws HiveException {
        final String path = stringOI.getPrimitiveJavaObject(objects[0]).toString();

        // 从zk获取数据
        if (zkValue == null) {
            try {
                zkValue = ZkUtils.getValue(path);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Iterator<String> it = zkValue.iterator();
        while (it.hasNext()) {
            String ob = it.next();
            forward(ob);
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
