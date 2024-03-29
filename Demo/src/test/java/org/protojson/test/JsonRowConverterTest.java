package org.protojson.test;

import java.io.IOException;

import org.protojson.runner.JsonRowConverter;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-10-27
 */

public class JsonRowConverterTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @Test
    public void testPerformance() throws IOException {
        String json = "{\"transcode_info\":{\"action\":\"MULTI_BITRATE_TRANSCODE\",\"clientInfo\":{\"bizDef\":null,"
                + "\"kpn\":null,\"appId\":null,\"bizName\":\"kuaishou\",\"tenant\":\"TRIGGER-single-roi\"},"
                + "\"jobId\":\"346469c5-b518-4fe1-8b2a-0cbe50d6fa51-20230826-0\",\"status\":\"COMPLETE\","
                + "\"startTime\":1693064893118,\"endTime\":1693066496704,\"elapsedTime\":1603586,"
                + "\"paramJson\":{\"passThrough\":{},\"requestId\":\"111472290636#10390469648\","
                + "\"bizInfo\":{\"bizName\":\"KUAISHOU\",\"bizId\":\"111472290636_fe3631af25e7a622_pre540pkvc26\","
                + "\"extra\":{\"photoId\":\"111472290636\",\"userId\":\"1662863413\",\"uploadSource\":\"APP\","
                + "\"isVipUser\":\"false\",\"jobCategory\":\"ASYNC_PRIORITY_QUEUE\","
                + "\"stereoType\":\"NOT_SPHERICAL_VIDEO\",\"userCheckStereoType\":\"NOT_SPHERICAL_VIDEO\","
                + "\"transcodeType\":\"pre540pkvc26\",\"triggerInfo\":\"{\\\"fanCount\\\":2053,"
                + "\\\"viewCount\\\":2301,\\\"triggerType\\\":\\\"ONLINE_ROI_SINGLE\\\",\\\"priority\\\":4.9183576E7,"
                + "\\\"cost\\\":3498.07,\\\"profit\\\":1.720475918871455E8,\\\"heatMap\\\":{\\\"HOURS_1\\\":420"
                + ".262307,\\\"MINUTES_30\\\":372.262307},\\\"heatTime\\\":1693062420000,"
                + "\\\"enqueueTime\\\":1693062438300,\\\"dequeueTime\\\":1693064886455}\"}},"
                + "\"input\":{\"type\":\"BLOBSTORE\",\"db\":\"video\",\"table\":\"def\",\"key\":\"111472290636"
                + ".mp4\"},\"output\":{\"type\":\"BLOBSTORE\",\"db\":\"video\",\"table\":\"def\","
                + "\"key\":\"111472290636_pre540pkvc26.mp4\"},\"overwrite\":null,"
                + "\"templateId\":\"CONFIG_ID_KVC26_FIREFLY_540P30FPS_PREDCODE\",\"skipOutputCheck\":null,"
                + "\"customParamMap\":{\"globalId\":\"111472290636\",\"hlsKeyPrefix\":\"5228397800412224658\","
                + "\"bizFeature\":{\"sourceType\":\"\",\"primaryAuthorCategory\":\"颜值\","
                + "\"secondaryAuthorCategory\":\"其他颜值\"},\"audioWatermark\":\"111472290636\","
                + "\"visionFeature\":{\"blurProbability\":0.501757800579071,\"blockyProbability\":0"
                + ".28305619955062866,\"dirtylensProbability\":0.09090341627597809,\"colorfulness\":0"
                + ".3698700011663138,\"junkVideoProbability\":0.0,\"entropyAvg\":12.440396499633788,\"entropyVar\":50"
                + ".11938743591308,\"lowLightPercentage\":0.0,\"methodType\":\"Import\",\"phoneMode\":\"iPhone12,1\","
                + "\"lowQualityProbability\":0.14541522892717984,\"highQualityProbability\":0.0,\"version\":\"8\","
                + "\"vmafBoost\":0.0,\"qualityScore\":0.8545847710728202,\"defocusProbability\":0.15094909071922302,"
                + "\"clickThroughRateProbability\":-1.0,\"corruptionProbability\":0.012209574691951275,"
                + "\"noiseProbability\":0.1533203125,\"letterboxProbability\":6.238426794461308E-14,"
                + "\"letterboxTop\":[0.0,0.1,0.3,0.9,0.0,0.0,0.0,0.0,0.0,0.0],\"letterboxBottom\":[1.0,1.0,1.0,1.0,1"
                + ".0,1.0,1.0,1.0,1.0,1.0],\"aestheticsScore\":0.1398320198059082,\"normalProbability\":0"
                + ".996386706829071,\"pureProbability\":0.000001341104507446289,\"textProbability\":0"
                + ".0000032007694699132117,\"cartoonProbability\":0.0035543739795684814,\"qrcodeProbability\":0"
                + ".00008258223533630371,\"mosScore\":0.6389973759651184,\"capeEncRes\":-1.0,\"intraComplexity\":1"
                + ".1047329902648926,\"interComplexity\":0.6018604636192322,\"nonRoiSpatialComplexity\":-1.0,"
                + "\"roiSpatialComplexity\":0.0,\"blurProbabilityNew\":0.593701183795929,\"noiseProbabilityNew\":0"
                + ".234574094414711,\"interlaceRatio\":0.0,\"underexposedProbability\":2.384185791015625E-7,"
                + "\"overexposedProbability\":0.0011610150104388595,\"lowContrastProbability\":0.11131031811237335,"
                + "\"vrVideo\":0.0,\"r0Probability\":0.998583972454071,\"r90Probability\":0.0008095026132650673,"
                + "\"r270Probability\":0.000613307929597795,\"oritationProbability\":0.0,\"faceRatio\":0"
                + ".800000011920929,\"processedTime\":2.8326728070000002,\"processedFrames\":10.0,"
                + "\"processedTimePerFrame\":0.2832672807,\"frameInterval\":0,\"interlaceScaleClass\":-1.0,"
                + "\"blockyProbabilityNew\":0.35209569334983826,\"othersProbability\":1.0,\"skyProbability\":0.0,"
                + "\"mountainProbability\":0.0,\"fieldProbability\":0.0,\"woodProbability\":0.0,"
                + "\"waterProbability\":0.0,\"crowdProbability\":0.0,\"personProbability\":0.0,"
                + "\"portraitProbability\":1.0,\"carProbability\":1.1324882365215672E-7,\"writeProbability\":0.0,"
                + "\"foodProbability\":0.0,\"cgProbability\":2.9802322387695312E-8,\"nightProbability\":0.0,"
                + "\"purecolorProbability\":0.0,\"capProbability\":1.1920929132713809E-8,\"mobaProbability\":0.0,"
                + "\"fpsProbability\":0.0,\"capeMotionHorizon\":1.5776004433631896,\"capeMotionVertical\":1"
                + ".4595622658729552,\"idet\":0.0,\"idetInterlaceRatio\":0.0,\"idetTff\":0.0,\"idetBff\":0.0,"
                + "\"idetProgressive\":406.0,\"idetUndetermined\":0.0,\"stageProbability\":0.0,"
                + "\"photofilmProbability\":0.0,\"letterboxColorProbability\":0.0,\"letterboxBlurProbability\":0"
                + ".000003546476364135742,\"letterboxSimilarProbability\":0.22535906732082367,"
                + "\"letterboxColorTxtProbability\":0.0003959834575653076,\"letterboxBlurTxtProbability\":0"
                + ".11797485500574112,\"letterboxOtherProbability\":0.656298816204071,\"nrQualityScoreNN\":[3"
                + ".314453125,3.482421875,3.615234375,3.5703125,3.140625,1.787109375,1.9775390625,1.7861328125,1"
                + ".8525390625,2.005859375],\"gameProbability\":0.000017857551938504912,\"maxIntraComplexity\":1"
                + ".9540318250656128,\"minIntraComplexity\":0.30588826537132263,\"maxInterComplexity\":1"
                + ".2032749652862549,\"minInterComplexity\":0.30401289463043213,\"topIntraComplexity\":0"
                + ".9904209971427917,\"topInterComplexity\":0.6301864981651306,\"yMean\":0.0,\"yMeanMax\":0.0,"
                + "\"yMeanMin\":0.0,\"yDiffStd\":0.0,\"yDiffStdMax\":0.0,\"yDiffStdMin\":0.0,\"embedding\":[-10"
                + ".0390625,-13.671875,-6.04296875,-24.046875,-6.48828125,-7.84765625,-8.125,2.919921875,16.34375,-2"
                + ".92578125],\"scenecutNum\":0,\"nrQualityAvgScoreNN\":3.5559895038604736,\"frQualityScore\":[],"
                + "\"frQualityAvgScore\":0.0,\"frQualityScoreNN\":[],\"frQualityAvgScoreNN\":0.0,"
                + "\"drQualityScore\":[],\"drQualityAvgScore\":0.0,\"drQualityScoreNN\":[],\"drQualityAvgScoreNN\":0"
                + ".0,\"kvqScore\":0.0,\"vmaf\":0.0,\"vmafNeg\":0.0,\"psnr\":[],\"psnrAvg\":0.0,\"ssim\":[],"
                + "\"ssimAvg\":0.0,\"sharpness\":0.14013671875,\"corruptionProbs\":[0.002361297607421875,0"
                + ".004451751708984375,0.011474609375,0.0008807182312011719,0.0010471343994140625,0"
                + ".0015821456909179688,0.0022640228271484375,0.0014476776123046875,0.0003590583801269531,0"
                + ".0007004737854003906,0.01157379150390625,0.00460052490234375,0.0016107559204101562,0"
                + ".0017461776733398438,0.00420379638671875,0.0004467964172363281,0.002071380615234375,8"
                + ".7738037109375E-4,0.0002627372741699219,0.003879547119140625,0.013580322265625,0"
                + ".0020084381103515625,0.000232696533203125,0.000035762786865234375,0.0002574920654296875,0"
                + ".0000680088996887207,0.0011949539184570312,0.0012559890747070312],\"pureProbs\":[],"
                + "\"purecolorProbs\":[]},\"seiConfig\":{\"id\":null,\"ref\":null,\"expr\":null,"
                + "\"base64SeiInfo\":null},\"colorSpaceConfig\":{\"id\":null,\"ref\":null,\"expr\":null,"
                + "\"colorSpaceFilter\":null,\"colorSpace\":null,\"colorPrimaries\":null,\"colorTrc\":null},"
                + "\"sphericalConfig\":{\"id\":null,\"ref\":null,\"expr\":null,\"meta\":null},"
                + "\"timeConfig\":{\"id\":null,\"ref\":null,\"expr\":null,\"startTime\":null,\"endTime\":null}}},"
                + "\"resultJson\":{\"outputs\":[{\"status\":\"NORMAL\",\"id\":\"k26hm1\","
                + "\"outputFile\":{\"type\":\"BLOBSTORE\",\"db\":\"video\",\"table\":\"def\","
                + "\"key\":\"111472290636_k26hm1.mp4\"},\"mediaInfo\":{\"videoStreams\":[{\"codeType\":\"video\","
                + "\"codecName\":\"kvc26\",\"startTime\":0.0,\"duration\":13.566667,\"bitrate\":155634,"
                + "\"profile\":\"1\",\"width\":576,\"height\":1024,\"avgFrameRate\":\"2035000000/67833333\","
                + "\"pixFmt\":\"yuv420p\",\"hdrType\":\"SDR\",\"nbFrames\":407,\"avgFrameRateInDouble\":30"
                + ".00000014742015,\"rframeRate\":\"30/1\"}],\"audioStreams\":[{\"codeType\":\"audio\","
                + "\"codecName\":\"aac\",\"startTime\":0.0,\"duration\":13.513991,\"bitrate\":59606,"
                + "\"profile\":\"HE-AAC\",\"channels\":2,\"sampleRate\":44100}],"
                + "\"format\":{\"simpleFormatName\":\"MP4\",\"formatName\":\"mov,mp4,m4a,3gp,3g2,mj2\","
                + "\"formatLongName\":\"QuickTime / MOV\",\"size\":380341,\"duration\":13.629,\"bitRate\":223253,"
                + "\"tags\":{\"comment\":\"#[1080x1920][30.00fps][6857k][0k][C][tv=71ff_atlas-6.0"
                + ".0][uploadSource=APP][cape3.720->576][audioGain=0.0000]\",\"location\":null,"
                + "\"major_brand\":\"isom\",\"com.apple.quicktime.location.ISO6709\":null}},\"primaryColor\":null},"
                + "\"transcodeInfo\":{\"maxVideoBitrate\":2300},\"bitratePattern\":[423,208,423,84,100],"
                + "\"masterPlaylist\":null,\"playlistFileSize\":0},{\"status\":\"NORMAL\",\"id\":\"k26h1\","
                + "\"outputFile\":{\"type\":\"BLOBSTORE\",\"db\":\"video\",\"table\":\"def\","
                + "\"key\":\"111472290636_k26h1.mp4\"},\"mediaInfo\":{\"videoStreams\":[{\"codeType\":\"video\","
                + "\"codecName\":\"kvc26\",\"startTime\":0.0,\"duration\":13.566667,\"bitrate\":255657,"
                + "\"profile\":\"1\",\"width\":576,\"height\":1024,\"avgFrameRate\":\"2035000000/67833333\","
                + "\"pixFmt\":\"yuv420p\",\"hdrType\":\"SDR\",\"nbFrames\":407,\"avgFrameRateInDouble\":30"
                + ".00000014742015,\"rframeRate\":\"30/1\"}],\"audioStreams\":[{\"codeType\":\"audio\","
                + "\"codecName\":\"aac\",\"startTime\":0.0,\"duration\":13.513991,\"bitrate\":59606,"
                + "\"profile\":\"HE-AAC\",\"channels\":2,\"sampleRate\":44100}],"
                + "\"format\":{\"simpleFormatName\":\"MP4\",\"formatName\":\"mov,mp4,m4a,3gp,3g2,mj2\","
                + "\"formatLongName\":\"QuickTime / MOV\",\"size\":549964,\"duration\":13.629,\"bitRate\":322819,"
                + "\"tags\":{\"comment\":\"#[1080x1920][30.00fps][6857k][0k][C][tv=71ff_atlas-6.0"
                + ".0][uploadSource=APP][cape3.720->576][audioGain=0.0000]\",\"location\":null,"
                + "\"major_brand\":\"isom\",\"com.apple.quicktime.location.ISO6709\":null}},\"primaryColor\":null},"
                + "\"transcodeInfo\":{\"maxVideoBitrate\":2300},\"bitratePattern\":[681,305,681,128,164],"
                + "\"masterPlaylist\":null,\"playlistFileSize\":0}]},\"errorMsg\":null,\"env\":\"PROD\","
                + "\"executionInfoJson\":{\"region\":\"HB1\","
                + "\"serviceName\":\"video-mps-transcode-workflow-schedule-worker-fass\","
                + "\"resourceUsageInfos\":[{\"id\":\"k26hm1\",\"resourceType\":\"CPU\",\"resourceLevel\":\"OFFLINE\","
                + "\"avgResourceUsageRate\":0.0,\"avgResourceUsage\":230795404.04528296},{\"id\":\"k26h1\","
                + "\"resourceType\":\"CPU\",\"resourceLevel\":\"OFFLINE\",\"avgResourceUsageRate\":0.0,"
                + "\"avgResourceUsage\":251189658.38249874}],\"sourceHdrVideo\":false,"
                + "\"inputMediaInfo\":{\"videoStreams\":[{\"codeType\":\"video\",\"codecName\":\"hevc\","
                + "\"startTime\":0.0,\"duration\":13.566667,\"bitrate\":6753242,\"profile\":\"Main\",\"width\":1080,"
                + "\"height\":1920,\"avgFrameRate\":\"30/1\",\"pixFmt\":\"yuv420p\",\"hdrType\":\"SDR\","
                + "\"nbFrames\":0,\"avgFrameRateInDouble\":30.0,\"rframeRate\":\"30/1\"}],"
                + "\"audioStreams\":[{\"codeType\":\"audio\",\"codecName\":\"aac\",\"startTime\":0.0,\"duration\":13"
                + ".514014,\"bitrate\":192137,\"profile\":\"LC\",\"channels\":2,\"sampleRate\":44100}],"
                + "\"format\":{\"simpleFormatName\":\"MP4\",\"formatName\":\"mov,mp4,m4a,3gp,3g2,mj2\","
                + "\"formatLongName\":\"QuickTime / MOV\",\"size\":11938405,\"duration\":13.566667,"
                + "\"bitRate\":7039845,\"tags\":{\"comment\":null,\"location\":null,\"major_brand\":\"iso5\",\"com"
                + ".apple.quicktime.location.ISO6709\":null}},\"primaryColor\":null},"
                + "\"outputInfo\":{\"audioDuration\":13.513991,\"videoDuration\":13.566667,\"width\":576,"
                + "\"height\":1024,\"codecName\":\"kvc26\",\"fps\":30.00000014742015,\"hdrType\":\"SDR\","
                + "\"videoCodec\":\"KVC\",\"audioCodec\":\"aac\"},\"hdrTranscodeType\":null,\"enhanceAudio\":false,"
                + "\"resultInfos\":[{\"duration\":13.566667,\"frameRate\":30.00000014742015,\"codec\":\"LIB_KVC\","
                + "\"resolution\":576,\"cape\":true,\"hdrTranscode\":false,\"preset\":\"\"},{\"duration\":13.566667,"
                + "\"frameRate\":30.00000014742015,\"codec\":\"LIB_KVC\",\"resolution\":576,\"cape\":true,"
                + "\"hdrTranscode\":false,\"preset\":\"\"}],"
                + "\"transcodeInfos\":[{\"outputVideoTranscodeInfo\":{\"audioDuration\":13.513991,"
                + "\"videoDuration\":13.566667,\"width\":576,\"height\":1024,\"codecName\":\"kvc26\",\"fps\":30"
                + ".00000014742015,\"hdrType\":\"SDR\",\"videoCodec\":\"KVC\",\"audioCodec\":\"aac\"},"
                + "\"outputMediaInfo\":{\"videoStreams\":[{\"codeType\":\"video\",\"codecName\":\"kvc26\","
                + "\"startTime\":0.0,\"duration\":13.566667,\"bitrate\":155634,\"profile\":\"1\",\"width\":576,"
                + "\"height\":1024,\"avgFrameRate\":\"2035000000/67833333\",\"pixFmt\":\"yuv420p\","
                + "\"hdrType\":\"SDR\",\"nbFrames\":407,\"avgFrameRateInDouble\":30.00000014742015,"
                + "\"rframeRate\":\"30/1\"}],\"audioStreams\":[{\"codeType\":\"audio\",\"codecName\":\"aac\","
                + "\"startTime\":0.0,\"duration\":13.513991,\"bitrate\":59606,\"profile\":\"HE-AAC\",\"channels\":2,"
                + "\"sampleRate\":44100}],\"format\":{\"simpleFormatName\":\"MP4\",\"formatName\":\"mov,mp4,m4a,3gp,"
                + "3g2,mj2\",\"formatLongName\":\"QuickTime / MOV\",\"size\":380341,\"duration\":13.629,"
                + "\"bitRate\":223253,\"tags\":{\"comment\":\"#[1080x1920][30.00fps][6857k][0k][C][tv=71ff_atlas-6.0"
                + ".0][uploadSource=APP][cape3.720->576][audioGain=0.0000]\",\"location\":null,"
                + "\"major_brand\":\"isom\",\"com.apple.quicktime.location.ISO6709\":null}},\"primaryColor\":null},"
                + "\"transcodeConfigInfo\":{\"configId\":\"CONFIG_ID_KVC26_HIGHM_PREFILTER_CAPE_3\","
                + "\"outputId\":\"k26hm1\",\"hdrTranscodeType\":\"\",\"containerFormat\":\"\",\"preset\":\"firefly\","
                + "\"enhanceAudio\":false,\"addWatermark\":false,\"videoCodec\":\"KVC\","
                + "\"videoCodecLibrary\":\"libkvc\",\"audioCodec\":\"LIB_FDK_AAC\",\"audioCodecLibrary\":null},"
                + "\"transcodeTypeResourceUsageInfo\":[{\"id\":\"k26hm1\",\"resourceType\":\"CPU\","
                + "\"resourceLevel\":\"OFFLINE\",\"avgResourceUsageRate\":0.0,\"avgResourceUsage\":230795404"
                + ".04528296}]},{\"outputVideoTranscodeInfo\":{\"audioDuration\":13.513991,\"videoDuration\":13"
                + ".566667,\"width\":576,\"height\":1024,\"codecName\":\"kvc26\",\"fps\":30.00000014742015,"
                + "\"hdrType\":\"SDR\",\"videoCodec\":\"KVC\",\"audioCodec\":\"aac\"},"
                + "\"outputMediaInfo\":{\"videoStreams\":[{\"codeType\":\"video\",\"codecName\":\"kvc26\","
                + "\"startTime\":0.0,\"duration\":13.566667,\"bitrate\":255657,\"profile\":\"1\",\"width\":576,"
                + "\"height\":1024,\"avgFrameRate\":\"2035000000/67833333\",\"pixFmt\":\"yuv420p\","
                + "\"hdrType\":\"SDR\",\"nbFrames\":407,\"avgFrameRateInDouble\":30.00000014742015,"
                + "\"rframeRate\":\"30/1\"}],\"audioStreams\":[{\"codeType\":\"audio\",\"codecName\":\"aac\","
                + "\"startTime\":0.0,\"duration\":13.513991,\"bitrate\":59606,\"profile\":\"HE-AAC\",\"channels\":2,"
                + "\"sampleRate\":44100}],\"format\":{\"simpleFormatName\":\"MP4\",\"formatName\":\"mov,mp4,m4a,3gp,"
                + "3g2,mj2\",\"formatLongName\":\"QuickTime / MOV\",\"size\":549964,\"duration\":13.629,"
                + "\"bitRate\":322819,\"tags\":{\"comment\":\"#[1080x1920][30.00fps][6857k][0k][C][tv=71ff_atlas-6.0"
                + ".0][uploadSource=APP][cape3.720->576][audioGain=0.0000]\",\"location\":null,"
                + "\"major_brand\":\"isom\",\"com.apple.quicktime.location.ISO6709\":null}},\"primaryColor\":null},"
                + "\"transcodeConfigInfo\":{\"configId\":\"CONFIG_ID_KVC26_HIGH_PREFILTER_CAPE_3\","
                + "\"outputId\":\"k26h1\",\"hdrTranscodeType\":\"\",\"containerFormat\":\"\",\"preset\":\"firefly\","
                + "\"enhanceAudio\":false,\"addWatermark\":false,\"videoCodec\":\"KVC\","
                + "\"videoCodecLibrary\":\"libkvc\",\"audioCodec\":\"LIB_FDK_AAC\",\"audioCodecLibrary\":null},"
                + "\"transcodeTypeResourceUsageInfo\":[{\"id\":\"k26h1\",\"resourceType\":\"CPU\","
                + "\"resourceLevel\":\"OFFLINE\",\"avgResourceUsageRate\":0.0,\"avgResourceUsage\":251189658"
                + ".38249874}]}]},\"jobConfig\":{\"notificationConfig\":{\"notificationModel\":\"FINAL_STATE\","
                + "\"kafkaTopic\":null,\"passthrough\":null,\"partitionSelectorKey\":null},"
                + "\"priorityLevel\":\"DEFAULT_PRIORITY\",\"jobLevel\":\"IDLE\",\"pipeline\":\"default\"}},"
                + "\"transcode_result_info\":{\"actionType\":\"TRANSCODE_RESULT\",\"primaryKey\":\"111472290636\","
                + "\"videoId\":\"fe3631af25e7a622\",\"bizName\":\"kuaishou\","
                + "\"requestId\":\"111472290636#10390469648\",\"timestamp\":1693066498647,\"enhanceDecision\":null,"
                + "\"transcodeResult\":{\"transcodeType\":\"pre540pkvc26\","
                + "\"triggerInfo\":{\"triggerType\":\"ONLINE_ROI_SINGLE\",\"heatMap\":{\"HOURS_1\":420.262307,"
                + "\"HOURS_6\":null,\"HOURS_12\":null,\"DAYS_1\":null,\"MINUTES_30\":372.262307},\"heatKey\":null,"
                + "\"heatValue\":null,\"priority\":4.9183576E7,\"triggerTime\":1693062420000,"
                + "\"acceptTime\":1693062438300,\"startTime\":1693064886455,\"extra\":\"{\\\"fanCount\\\":2053,"
                + "\\\"viewCount\\\":2301,\\\"triggerType\\\":\\\"ONLINE_ROI_SINGLE\\\",\\\"priority\\\":4.9183576E7,"
                + "\\\"cost\\\":3498.07,\\\"profit\\\":1.720475918871455E8,\\\"heatMap\\\":{\\\"HOURS_1\\\":420"
                + ".262307,\\\"MINUTES_30\\\":372.262307},\\\"heatTime\\\":1693062420000,"
                + "\\\"enqueueTime\\\":1693062438300,\\\"dequeueTime\\\":1693064886455}\"},"
                + "\"tenant\":\"TRIGGER-single-roi\",\"enhanceType\":null}},"
                + "\"explode_output_info\":{\"status\":\"NORMAL\",\"id\":\"k26h1\","
                + "\"outputFile\":{\"type\":\"BLOBSTORE\",\"db\":\"video\",\"table\":\"def\","
                + "\"key\":\"111472290636_k26h1.mp4\"},\"mediaInfo\":{\"videoStreams\":[{\"codeType\":\"video\","
                + "\"codecName\":\"kvc26\",\"startTime\":0.0,\"duration\":13.566667,\"bitrate\":255657,"
                + "\"profile\":\"1\",\"width\":576,\"height\":1024,\"avgFrameRate\":\"2035000000/67833333\","
                + "\"pixFmt\":\"yuv420p\",\"hdrType\":\"SDR\",\"nbFrames\":407,\"avgFrameRateInDouble\":30"
                + ".00000014742015,\"rframeRate\":\"30/1\"}],\"audioStreams\":[{\"codeType\":\"audio\","
                + "\"codecName\":\"aac\",\"startTime\":0.0,\"duration\":13.513991,\"bitrate\":59606,"
                + "\"profile\":\"HE-AAC\",\"channels\":2,\"sampleRate\":44100}],"
                + "\"format\":{\"simpleFormatName\":\"MP4\",\"formatName\":\"mov,mp4,m4a,3gp,3g2,mj2\","
                + "\"formatLongName\":\"QuickTime / MOV\",\"size\":549964,\"duration\":13.629,\"bitRate\":322819,"
                + "\"tags\":{\"comment\":\"#[1080x1920][30.00fps][6857k][0k][C][tv=71ff_atlas-6.0"
                + ".0][uploadSource=APP][cape3.720->576][audioGain=0.0000]\",\"location\":null,"
                + "\"major_brand\":\"isom\",\"com.apple.quicktime.location.ISO6709\":null}},\"primaryColor\":null},"
                + "\"transcodeInfo\":{\"maxVideoBitrate\":2300},\"bitratePattern\":[681,305,681,128,164],"
                + "\"masterPlaylist\":null,\"playlistFileSize\":0},"
                + "\"explode_transcode_info\":{\"outputVideoTranscodeInfo\":{\"audioDuration\":13.513991,"
                + "\"videoDuration\":13.566667,\"width\":576,\"height\":1024,\"codecName\":\"kvc26\",\"fps\":30"
                + ".00000014742015,\"hdrType\":\"SDR\",\"videoCodec\":\"KVC\",\"audioCodec\":\"aac\"},"
                + "\"outputMediaInfo\":{\"videoStreams\":[{\"codeType\":\"video\",\"codecName\":\"kvc26\","
                + "\"startTime\":0.0,\"duration\":13.566667,\"bitrate\":255657,\"profile\":\"1\",\"width\":576,"
                + "\"height\":1024,\"avgFrameRate\":\"2035000000/67833333\",\"pixFmt\":\"yuv420p\","
                + "\"hdrType\":\"SDR\",\"nbFrames\":407,\"avgFrameRateInDouble\":30.00000014742015,"
                + "\"rframeRate\":\"30/1\"}],\"audioStreams\":[{\"codeType\":\"audio\",\"codecName\":\"aac\","
                + "\"startTime\":0.0,\"duration\":13.513991,\"bitrate\":59606,\"profile\":\"HE-AAC\",\"channels\":2,"
                + "\"sampleRate\":44100}],\"format\":{\"simpleFormatName\":\"MP4\",\"formatName\":\"mov,mp4,m4a,3gp,"
                + "3g2,mj2\",\"formatLongName\":\"QuickTime / MOV\",\"size\":549964,\"duration\":13.629,"
                + "\"bitRate\":322819,\"tags\":{\"comment\":\"#[1080x1920][30.00fps][6857k][0k][C][tv=71ff_atlas-6.0"
                + ".0][uploadSource=APP][cape3.720->576][audioGain=0.0000]\",\"location\":null,"
                + "\"major_brand\":\"isom\",\"com.apple.quicktime.location.ISO6709\":null}},\"primaryColor\":null},"
                + "\"transcodeConfigInfo\":{\"configId\":\"CONFIG_ID_KVC26_HIGH_PREFILTER_CAPE_3\","
                + "\"outputId\":\"k26h1\",\"hdrTranscodeType\":\"\",\"containerFormat\":\"\",\"preset\":\"firefly\","
                + "\"enhanceAudio\":false,\"addWatermark\":false,\"videoCodec\":\"KVC\","
                + "\"videoCodecLibrary\":\"libkvc\",\"audioCodec\":\"LIB_FDK_AAC\",\"audioCodecLibrary\":null},"
                + "\"transcodeTypeResourceUsageInfo\":[{\"id\":\"k26h1\",\"resourceType\":\"CPU\","
                + "\"resourceLevel\":\"OFFLINE\",\"avgResourceUsageRate\":0.0,\"avgResourceUsage\":251189658"
                + ".38249874}]},\"explode_resource_usage_info\":{\"id\":\"k26h1\",\"resourceType\":\"CPU\","
                + "\"resourceLevel\":\"OFFLINE\",\"avgResourceUsageRate\":0.0,\"avgResourceUsage\":251189658"
                + ".38249874},\"transcode_id\":\"k26h1\",\"request_id\":\"111472290636#10390469648\"}\n";
        String[] params = new String[] {"transcode_info.resultJson.outputs.1.mediaInfo.videoStreams.0.codecName","transcode_info.resultJson.outputs.1.outputFile","transcode_info.resultJson.outputs.0.outputFile","transcode_info.paramJson.customParamMap.visionFeature.letterboxTop.1","transcode_info.paramJson.customParamMap.visionFeature.letterboxTop.0","explode_transcode_info.transcodeConfigInfo.configId", "explode_transcode_info.transcodeConfigInfo.outputId","explode_output_info.mediaInfo.format.size", "explode_output_info.mediaInfo.format.duration","explode_output_info.mediaInfo.format.bitRate","explode_resource_usage_info.avgResourceUsage","transcode_info.startTime","transcode_info.endTime","transcode_info.elapsedTime","transcode_info.action","transcode_result_info.primaryKey",  "transcode_result_info.transcodeResult.triggerInfo.triggerType",  "transcode_result_info.transcodeResult.triggerInfo.heatMap.minutes_30",  "transcode_result_info.transcodeResult.enhanceType",  "transcode_result_info.transcodeResult.tenant",  "transcode_result_info.transcodeResult.triggerInfo.extra.fanCount",   "transcode_result_info.transcodeResult.triggerInfo.extra.viewCount",   "transcode_result_info.transcodeResult.triggerInfo.extra.triggerType",  "transcode_result_info.transcodeResult.triggerInfo.extra.heatMap",  "transcode_result_info.transcodeResult.triggerInfo.extra.heatTime",  "transcode_result_info.transcodeResult.triggerInfo.extra.enqueueTime",  "transcode_result_info.transcodeResult.triggerInfo.extra.dequeueTime"};
        JsonRowConverter converter = new JsonRowConverter(params);
        String[] result = converter.process(json);
        System.out.println(String.join(";\n", result));

    }
}
