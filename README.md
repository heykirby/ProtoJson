
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
heave_dependency_udf和platform_udf模块已迁移至http://git.corp.kuaishou.com/data-platform/platform_udf


