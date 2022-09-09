package com.kuaishou.data.udf.video.utils;

import javax.annotation.Nonnull;

import com.kuaishou.framework.jedis.JedisClusterConfig;

import kuaishou.common.BizDef;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-09-02
 */
public enum RedisConfigKey implements JedisClusterConfig {
    FLINK_CALC_STORE("videoFlinkCalculationStore"),
    FEED_BACK_TAG_FAQ_INFO("videoFeedBackTagAndFaqInfo"),
    ORIGIN_DOMAIN_CACHE("CdnDomainCached"),
    VOD_P2SP_PHOTO_ID_INFO("videoVodP2spHotVideo"),
    VOD_P2SP_TRACKER_INFO("p2pWarmupData"),
    HOST_IP_WARM_UP("videoCdnResourceWarmUp"),
    DASHBOARD_2020_YZ("dashboard2020yz"),
    DASHBOARD_2020_ZL("dashboard2020zl"),
    VIDEO_USER_RATE_LIMITING_STAT("videoUserRateLimitingStat"),
    OVERSEA_VORAGE_COUNTRY_CODE_INFO("overseaVoyageCountryCodeInfo"),
    PHOTO_VIDEO_PLAYER_TOPN("photoVideoPlayerTopN"),
    SNACK_VIDEO_SPG_TRANSCODER("snackVideoSpgTranscoder"),
    PHOTO_VIDEO_PLAYER_TOP_N("photoVideoPlayerTopN"),
    HOTMODEL_AUTHOR_INFO("videoHotmodelAuthorInfo"),
    KWAI_VIDEO_ASYNC_TRANSCODER("kwaiVideoAsyncTranscoder"),
    VIDEO_ALERT_MESSAGE_STAGE("videoAlertMessageStage"),
    WARM_UP_STAT("videoWarmupStat"),
    AB_KS_CHANGE_LOG("CrashMonitorAbKsChangeLog")
    //    WARM_UP_STAT_STAGING("videoWarmupStat_staging")
    ;

    private String bizName;

    RedisConfigKey(String bizName) {
        this.bizName = bizName;
    }

    @Override
    public String bizName() {
        return this.bizName;
    }

    @Override
    public boolean usingNewZk() {
        return true;
    }


    @Nonnull
    @Override
    public BizDef bizDef() {
        return BizDef.VIDEO;
    }
}
