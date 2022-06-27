package com.kuaishou.data.udf.video;

import static com.kuaishou.cdn.scope.CdnDispatchScope.getCdnIpLocation;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.http.util.TextUtils;

import com.kuaishou.cdn.dto.CdnIpLocationDTO;
import com.kuaishou.zt.base.common.scope.EndpointIp;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-06-23
 */
public class ParseIp extends UDF{
    public static String evaluate(String ip){
        return parseIp(ip).ispName();
    }
    private static EndpointIp getEndpointIp(String ip) {
        String ipv4 = null;
        String ipv6 = null;
        if (!TextUtils.isEmpty(ip) && ip.contains(":")) {
            ipv6 = ip;
        } else {
            ipv4 = ip;
        }
        return EndpointIp.from(ipv4, ipv6);
    }
    private static CdnIpLocationDTO parseIp(String ip) {
        return getCdnIpLocation(getEndpointIp(ip));
    }

}
