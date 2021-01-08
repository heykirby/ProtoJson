package com.kuaishou.data.udf.video;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ZhihuProductName2AppName extends UDF {
    private static Map<String, String> appNameMap = new HashMap<>();
    public String evaluate(String productName) {
//        if(appNameMap == null){
//            try{
//                String url =
//                        "jdbc:hive2://hiveserver-zk1.internal:2181,hiveserver-zk2.internal:2181,hiveserver-zk3"
//                                + ".internal:2181,hiveserver-zk4.internal:2181,hiveserver-zk5.internal:2181/default;"
//                                + "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2?task.group.id=14;"
//                                + "kuaishou.bigdata.job.realuser=chenxinwei";
//                String user = "chenxinwei"; // 用户名，请使用个人账号
//                String pwd = ""; // 密码，置为空即可。
//                String driver = "hive2";
//                String auth = "sasl";
//
//                Properties prop = new Properties();
//                prop.setProperty(Utils.JdbcConnectionParams.AUTH_TYPE, auth);
//                prop.setProperty(Utils.JdbcConnectionParams.PROPERTY_DRIVER, driver);
//                prop.setProperty(Utils.JdbcConnectionParams.AUTH_USER, user);
//                prop.setProperty(Utils.JdbcConnectionParams.AUTH_PASSWD, pwd);
//                prop.setProperty("hiveconf:task.group.id", "14");
//
//                HiveDriver client = new HiveDriver();
//                Connection conn = client.connect(url, prop);
//                final Statement stat = conn.createStatement();
//                Calendar calendar = Calendar.getInstance();
//                calendar.add(Calendar.DAY_OF_MONTH, -1);
//                SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
//                String dateStr = format1.format(calendar.getTime());
//                final String sql =
//                        "select app_key,name from ks_origin_plat_db.infra_oauth_config_app_info_dt_snapshot where dt='"
//                                + dateStr + "'";
//                boolean ok = stat.execute(sql);
//                if (ok) {
//                    ResultSet ret = stat.getResultSet();
//                    appNameMap = new HashMap<>();
//                    while (ret.next()) {
//                        String appKey = ret.getString("app_key");
//                        String appName = ret.getString("name");
//                        appNameMap.put(appKey, appName);
//                    }
//                }
//                stat.close();
//                conn.close();
//            }catch (Exception e){
//                throw new RuntimeException(e);
//            }
//        }
        String s = "UNKNOWN";
        if(appNameMap.containsKey(productName)){
            s = appNameMap.get(productName);
        }
        return s;
    }
}
