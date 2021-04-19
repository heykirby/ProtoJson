package com.kuaishou.data.udf.video;

import com.google.common.util.concurrent.FluentFuture;
import com.kuaishou.mediacloud.MediaCloudClient;
import com.kuaishou.mediacloud.VideoDetail;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.concurrent.ExecutionException;

public class GetVideoId extends UDF {
    public String evaluate(String bizName, Long taskId){
        if (bizName == null || taskId == null){
            return null;
        }


        try {
            FluentFuture<VideoDetail> future = MediaCloudClient
                    .getInstance(bizName)
                    .getMediaApi()
                    .getVideoDetail(taskId.longValue());
            VideoDetail videoDetail = future.get();
            return videoDetail.getVideoId();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }
}
