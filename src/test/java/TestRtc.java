import java.io.IOException;

import org.junit.Test;

import com.kuaishou.data.udf.video.ExtractRtcCallDuration;

/**
 * @author yehe <yehe@kuaishou.com>
 * Created on 2021-09-10
 */
public class TestRtc {
    @Test
    public void testRtc() throws IOException {
        String params = "{\"details\":{\"down\":{\"v\":{\"s0\":{\"width\":1080,\"height\":1920},"
                + "\"s1\":{\"width\":580,\"height\":720}}}}}";
        String params1 = "{\"details\":{\"down\":{\"v\":{}}}}";
        System.out.println(ExtractRtcCallDuration.evaluate(params1));
    }
}
