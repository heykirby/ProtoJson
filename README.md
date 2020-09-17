
### [开发指北]

`platform_udf`模块下提供了一个标准UDF的代码，其他UDF开发可以参考。

#### UDAF开发参考
链接： [https://cwiki.apache.org/confluence/display/Hive/GenericUDAFCaseStudy](https://cwiki.apache.org/confluence/display/Hive/GenericUDAFCaseStudy)

#### UFTF开发参考
链接：[https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide+UDTF](https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide+UDTF)

#### 建议
尽量在自己的UDF中实现main方法测试自己UDF的功能(可参考`platform_udf`模块`Md5`函数的实现方式)，在符合预期的情况下，在进入下一步。

*push代码时请勿添加除`src`目录和`pom.xml`范围之外的文件。*

### [文档维护]
在链接：[https://wiki.corp.kuaishou.com/pages/viewpage.action?pageId=17172862] 页面下已为各个组创建了子页面，需要把本组开发的UDF说明和测试样例简要记录。
**写udf说明文档是必要步骤**

### [使用指南]
参考流程

1. 执行`mvn package`；
2. 进入target目录，找到生成的jar包，上传到`HDFS`；
3. 添加jar包，`add jar /path/to/jar/file`；
4. 创建函数，`create function function_name as 'com.kuaishou.data.hive.udf.platform.class_name'`，其中`function_name`可以是自己想要使用的函数名称，尽量不要与hive的内建UDF名称重复。`class_name`表示项目中实现函数功能的类名称；
5. 至此，就可以使用创建的临时函数自由玩耍了。


视频组的jar包路径在`HDFS`的`/home/video/jars`。

### <font color=red>[备注]</font>
hue下执行自定义udf，如果出现错误，可以尝试 set hive.smart.router.enable=false;set hive.execution.engine=mr;
heave_dependency_udf和platform_udf模块已迁移至http://git.corp.kuaishou.com/data-platform/platform_udf

### <font color=red>[UDF]</font>
ADD jar viewfs:///home/video/jars/media-data-udf-1.0-SNAPSHOT-shaded.jar;
create temporary function parUrl as `$package`;

|序号| 分类 |  输入 |输出|package|说明|
|:-----:|:-----:| :----: | :----:|:----| :----:|
|1| 解析photoid |string url| long |com.kuaishou.data.udf.video.ParsePhotoId|输入为null ,0 或者解析错误，返回0|
|2| acfun域名解析taskid | string url| string |com.kuaishou.data.udf.video.ParseAcfunURLId|输入为null 抛出异常，解析错误 返回null|
|3|acfun域名解析subtask|string url|string|com.kuaishou.data.udf.video.ParseAcfunURL|输入为null 抛出异常，解析错误 返回null|
|4|pts解析|String str,Integer base|Integer|com.kuaishou.data.udf.video.ParsePTS|判断pts>Integer的值,返回1，否则返回0|
|5|pts解析|String str, Integer base|string|com.kuaishou.data.udf.video.PtsCount|返回满足ptsList.get(i) - ptsList.get(i - 2) > base 数量|
|6|pts解析|String str, Integer base|string|com.kuaishou.data.udf.video.PtsCount2|返回满足ptsList.get(i) + ptsList.get(i - 1) > base 数量|
|7|pts解析|Text pts,Text timesatmp|list|com.kuaishou.data.udf.video.PtsDetail|返回pts大于timestamp 的详细数据|
|8|pts解析|string|string|com.kuaishou.data.udf.video.PtsDuration|输入为null 抛出异常，解析错误 返回null|
|9|获取zk数据|String path|列表|com.kuaishou.data.udf.video.GetZKList|输入为null 抛出异常，解析错误 返回null|
|10|判断是否在zk列表里|String path,string id|boolean|com.kuaishou.data.udf.video.IsZKList|验证id是否包在path的zk里面，|
|11| 解析精彩片段id | long id OR String id | String id OR long id | com.kuaishou.data.udf.video.GetLiveHighlightId | 解析错误时返回 0 或者 "" |
|12| 判断url是否为有效的pkey url | String url,String domain, long timestamp | com.kuaishou.data.udf.video.AcfunJudgePkeyValid | 返回是否为有效pkey url的布尔值 |
