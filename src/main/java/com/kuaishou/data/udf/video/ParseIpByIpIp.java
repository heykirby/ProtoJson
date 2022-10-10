package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.ip.dto.IpResult;
import com.kuaishou.ip.util.IpUtils;
import com.kuaishou.zt.base.common.scope.EndpointIp;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-09-29
 */
public class ParseIpByIpIp extends UDF {
    public String evaluate(String ip) {
        String country = "";
        String province = "";
        String city = "";
        if (ip != null && !"".equals(ip)) {
            IpResult result;
            try {
                if (ip.contains(":")) {
                    result = IpUtils.getIpResult(EndpointIp.from(null, ip));
                } else {
                    result = IpUtils.getIpResult(EndpointIp.fromIPv4(ip));
                }
                country = result.getCountry() == null ? country : result.getCountry().getChineseName();
                province = result.getProvince() == null ? province : result.getProvince().name();
                city = result.getCity() == null ? city : result.getCity();
            } catch (Exception e) {
            }
        }
        return String.format("%s\t%s\t%s", country, province, city);
    }
}
