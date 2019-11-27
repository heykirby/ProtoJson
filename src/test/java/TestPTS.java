import java.util.ArrayList;
import java.util.Base64;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.junit.Test;

/**
 * @author xiazehui <xiazehui@kuaishou.com>
 * Created on 2019-09-06
 */
public class TestPTS {
    public static ArrayList<Long> evaluate(String str) throws UDFArgumentException {

        if (str == null || str.length() == 0) {
            throw new UDFArgumentException("must take two arguments");
        }

        ArrayList<Long> ptsList = new ArrayList<>();
        if (str.contains(",")) {
            for (String s : str.split(",")) {
                try {
                    ptsList.add(Long.parseLong(s));
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        } else {
            byte[] bytes = null;
            try {
                bytes = Base64.getDecoder().decode(str);
            } catch (IllegalArgumentException e) {
                return null;
            }
            long s = 0;
            for (int i = 0; i < bytes.length; i += 2) {
                s = (bytes[i] << 8) | (bytes[i + 1] & 0xff);
                ptsList.add(s);
            }
        }
        System.out.println(ptsList);
        String dur = "";
        for (int i = 1; i < ptsList.size(); i += 1) {
            long l = ptsList.get(i) - ptsList.get(i - 1);
            dur = dur + "," + String.valueOf(l);
        }
        return ptsList;
    }

    public static void main(String[] args) throws UDFArgumentException {
        ArrayList<Long>
                arr =
                evaluate(
                        "AAAAygBjAGUAZABkAGQAZQBfAGYAZABkAGMAZQBkAGQAZgBlAGMAZgBhAGMAZABjAGgAZQBkAGIAYABoAGIAZABmAGIAZQBkAGMAYQBmAGIAYwBlAGcAYgBjAGcAYgBnAGMAYQBlAGMAZgBhAGoAXwBmAGUAYQBhAGUAZABmAGMAYgBnAGEAZgBiAGUAZQBkAGUAYgBkAGQAZABmAGQAZgBjAGIAZABpAGMAXgBlAGY");
        int a=0;
        for (int i = 1; i < arr.size(); i++) {
            System.out.println(i + ":" + arr.get(i));
            if(arr.get(i)+arr.get(i-1)>200){
                a+=1;
            }
        }
        System.out.println( a);
    }
}
