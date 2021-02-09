import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import com.google.common.collect.ImmutableList;

/**
 * @author xiazehui <xiazehui@kuaishou.com>
 * Created on 2019-09-06
 */
public class TestPTS {


    public static List<ImmutableList<Long>> evaluate(String str, String base) throws UDFArgumentException {

        if (str == null || str.length() == 0) {
            throw new UDFArgumentException("must take two arguments");
        }
        Long baseTimesatmp = Long.valueOf(base);

        List<Long> ptsList = new ArrayList<>();
        List<Long> ptsList111 = new ArrayList<>();
        List<ImmutableList<Long>> ptsDetail = new ArrayList<>();
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
                s += (bytes[i] << 8) | (bytes[i + 1] & 0xff);
                ptsList.add(s);
            }

            long l = 0;
            for (int i = 0; i < bytes.length; i += 2) {
                l = (bytes[i] << 8) | (bytes[i + 1] & 0xff);
                ptsList111.add(l);
            }
        }
        System.out.println(ptsList);
        System.out.println(ptsList111);
        String dur = "";
        for (int i = 2; i < ptsList.size(); i += 1) {
            if (ptsList.get(i) - ptsList.get(i - 2) > 200) {
                ptsDetail.add(ImmutableList.of(ptsList.get(i - 2), ptsList.get(i)));
            }
        }
        return ptsDetail;
    }

    public static void main(String[] args) throws UDFArgumentException {
        List<String> items = new ArrayList<>();
        items.add("A");
        items.add("B");
        items.add("C");
        items.add("D");
        items.add("E");
        String join = StringUtils.join(items, ",");
        System.out.println(join);

        List<ImmutableList<Long>>
                arr =
                evaluate(
                        "AAAAygBjAGUAZABkAGQAZQBfAGYAZABkAGMAZQBkAGQAZgBlAGMAZgBhAGMAZABjAGgAZQBkAGIAYABoAGIAZABmAGIAZQBkAGMAYQBmAGIAYwBlAGcAYgBjAGcAYgBnAGMAYQBlAGMAZgBhAGoAXwBmAGUAYQBhAGUAZABmAGMAYgBnAGEAZgBiAGUAZQBkAGUAYgBkAGQAZABmAGQAZgBjAGIAZABpAGMAXgBlAGY",
                        "1575882676000");
        System.out.println(arr);
    }
}
