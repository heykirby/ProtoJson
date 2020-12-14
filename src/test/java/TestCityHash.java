import com.kuaishou.data.udf.video.CityHashUDF;
import org.junit.Test;

/**
 * @author wuxuexin <wuxuexin@kuaishou.com>
 * Created on 2020-12-14
 */
public class TestCityHash {

    @Test
    public void test() {
        CityHashUDF cityHashUDF = new CityHashUDF();
        System.out.println(cityHashUDF.evaluate("1554947804033", 1411382819L));
    }
}
