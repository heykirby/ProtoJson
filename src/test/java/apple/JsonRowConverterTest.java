package apple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.protojson.runner.JsonRowConverter;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2023-02-06
 */
public class JsonRowConverterTest {
    @Test
    public void testArray() throws IOException {
        String str0 = "[\"http://bd2.a.kwimgs"
                + ".com/upic/2022/12/17/17/BMjAyMjEyMTcxNzAwMDlfNTQwNjcxNzIzXzkxMzMyNDM3OTE1XzFfMw"
                + "==_ff_Bf62aa5a8e6c8f173818fc34a165a5824"
                + ".heif?tag=1-1674074347-d-0-egirchve8y-7df2338d7e55a200&type=hot&clientCacheKey=3xd5rkte7bgnnra_ff"
                + ".heif&di=13e9ede&bp=10001\",\"http://js2.a.yximgs"
                + ".com/upic/2022/12/17/17/BMjAyMjEyMTcxNzAwMDlfNTQwNjcxNzIzXzkxMzMyNDM3OTE1XzFfMw"
                + "==_ff_Bf62aa5a8e6c8f173818fc34a165a5824"
                + ".heif?tag=1-1674074347-d-1-sqdsymbuuc-8cc375b5b927a56f&type=hot&clientCacheKey=3xd5rkte7bgnnra_ff"
                + ".heif&di=13e9ede&bp=10001\"]";
        String str1 = "[{\"cdn\":\"txdwk.a.yximgs.com\",\"url\":\"http:\\/\\/txdwk.a.yximgs"
                + ".com\\/bs2\\/emotion\\/app_1574675492000_5xe88vmt2g36cxi.png\"},{\"cdn\":\"ali2.a.yximgs.com\","
                + "\"url\":\"http:\\/\\/ali2.a.yximgs.com\\/bs2\\/emotion\\/app_1574675492000_5xe88vmt2g36cxi"
                + ".png\"}]\n";
        String str2 = "";
        List<String> list = new ArrayList<>();
        list.add(str0);
        list.add(str2);
        list.add(str1);
        JsonRowConverter converter = new JsonRowConverter("0", "1", "1.cdn");
        for (String tmp : list) {
            try {
                String[] process = converter.process(tmp);
                for (String p : process) {
                    System.out.print(p + " ");
                }
            } catch (Exception e) {
                // ignore
                e.printStackTrace();
            } finally {
                System.out.println();
            }
        }
    }

    @Test
    public void testJson() throws IOException {
        String str = "{\"sdk_ver\":\"4.7.1.0\",\"config\":{\"max_retry_count\":1,\"bitmap_type\":\"RGB_565\","
                + "\"max_decoded_mem_cache_size\":74658610,\"max_encoded_mem_cache_size\":4194304,"
                + "\"max_disk_cache_size\":52428800},\"options\":{\"url\":\"http://p4.a.yximgs"
                + ".com/uhead/AB/2022/05/15/06/BMjAyMjA1MTUwNjAwMDRfNDAzMDAxNDFfOTUyOTEyNDEwNV9sdg==.kpg\","
                + "\"urls\":[],\"view_exists\":true,\"view_width\":413,\"view_height\":538,\"ratio\":1},"
                + "\"meta\":{\"format\":\"KPG\",\"size\":1808640,\"width\":720,\"height\":1256,\"frame_count\":1},"
                + "\"stat\":{\"status\":\"success\",\"data_source\":\"network\",\"error_message\":\"\","
                + "\"first_screen\":208,\"stay_duration\":515},\"cache\":{\"cost\":27,"
                + "\"decoded_mem_cached_count\":51,\"decoded_mem_cached_size\":73667368,"
                + "\"encoded_mem_cached_count\":100,\"encoded_mem_cached_size\":4126594,\"disk_cached_count\":503,"
                + "\"disk_cached_size\":23406114},\"network\":{\"status\":\"success\",\"cost\":66,\"url\":\"http://p4"
                + ".a.yximgs.com/uhead/AB/2022/05/15/06/BMjAyMjA1MTUwNjAwMDRfNDAzMDAxNDFfOTUyOTEyNDEwNV9sdg==.kpg\","
                + "\"server_ip\":\"121.17.124.106\",\"host\":\"p4.a.yximgs.com\",\"error_message\":\"\","
                + "\"retry_count\":0,\"http_code\":200,\"requests\":[{\"status\":\"success\",\"error_message\":\"\","
                + "\"url\":\"http://p4.a.yximgs"
                + ".com/uhead/AB/2022/05/15/06/BMjAyMjA1MTUwNjAwMDRfNDAzMDAxNDFfOTUyOTEyNDEwNV9sdg==.kpg\","
                + "\"http_code\":200,\"server_ip\":\"121.17.124.106\",\"protocol\":\"http/1.1\","
                + "\"received_bytes\":51889,\"cost\":57,\"dns_cost\":-1,\"connect_cost\":0,"
                + "\"waiting_response_cost\":31,\"response_cost\":17}]},\"decode\":{\"status\":\"success\","
                + "\"cost\":64,\"width\":1280,\"height\":720,\"bitmap_type\":\"RGB_565\"},"
                + "\"bs_info\":{\"biz_ft\":\"video\",\"biz_extra\":{\"sub_solution\":\"KRN\","
                + "\"bundle_id\":\"xxxxxxxxxxx\",\"up_biz_ft\":\"FT_Feed\"},\"scene\":\"feed_cover\"},"
                + "\"sys_prof\":{\"in_background\":false,\"mem_usage\":0.1311},"
                + "\"extra_message\":{\"controller_id\":\"163\",\"request_id\":\"162\"}}";
        JsonRowConverter converter = new JsonRowConverter(
                "sdk_ver",
                "config",
                "config.max_disk_cache_size",
                "network.request",
                "network.requests",
                "network.requests",
                "network.requests.3",
                "extra_message",
                "bs_info.biz_ft",
                "bs_info.biz_extra",
                "network.requests.0",
                "network.requests.0.status",
                "network.requests.0.server_ip"
        );
        String[] process = converter.process(str);
        System.out.println(String.join("\n\n", process));
    }

    @Test
    public void testSgp() throws IOException {
        String str = "{\"is_aemon\":0,\n\"no_aemon_reason\":1,\"ver\":\"5.0.7.15.6083.32441b231e\","
                + "\"config\":{\"player_type\":1,\"url\":\"http:\\/\\/98.98.13"
                + ".102\\/upic\\/2022\\/11\\/27\\/19"
                + "\\/BMjAyMjExMjcxOTU0NDBfMTUwMDAwMDgxMTE2NDEyXzE1MDEwMTQxOTUyMjMwMl8yXzM"
                + "=_ohd37_B26ea02913cf163dc7568cea247e9573f"
                + ".mp4?tt=ohd37&clientCacheKey=54ea8ac719d9d99615a948139da48817&tag=1-1675888452-h-0-jh38br6zsv"
                + "-67e085992dda5505\",\"host\":\"N\\/A\",\"domain\":\"98.98.13.102\",\"server_ip\":\"98.98.13.102\","
                + "\"transcode_type\":\"ohd37\",\"quality_type\":\"ohd\",\"pre_load\":false,\"pre_load_ms\":0,"
                + "\"pre_load_finish\":false,\"cache\":true,\"enable_accurate_seek\":true,"
                + "\"overlay_format\":\"SDL_FCC__AMC\",\"enable_manfiest_retry\":true,\"use_kpmid_player\":true,"
                + "\"max_buffer_size\":15728640,\"max_buffer_dur\":60000,\"max_buffer_strategy\":1,"
                + "\"bsp_buffer_strategy\":0,\"max_buffer_dur_bsp\":3000,\"mark_kbps\":-1,"
                + "\"first_high_water_mark\":100,\"next_high_water_mark\":1000,\"last_high_water_mark\":5000,"
                + "\"seek_high_water_mark\":100,\"use_kpmid\":true,\"product_ctx\":{\"biz_type\":\"internal\","
                + "\"play_index\":1},\"input_type\":0,\"enable_cdn_retry\":true},\"data_read\":{\"open_input\":32768,"
                + "\"find_stream_info\":209003,\"fst_a_pkt\":209003,\"fst_v_pkt\":209003,\"read_total\":2487524},"
                + "\"meta\":{\"fps\":29.58329963684082,\"dur\":30047,\"a_dur\":29932,"
                + "\"comment\":\"#[720x1280][enhanced][DeB_V3_INT8]#[720x1280][29"
                + ".58fps][23614k][96k][C][tv=3d0e_atlas-5.5.3][audioGain=0.0000][cape]\",\"channels\":2,"
                + "\"sample_rate\":44100,\"codec_a\":\"libfdk_aac, aac\",\"codec_v\":\"OMX.MTK.VIDEO.DECODER.HEVC, "
                + "hevc\",\"width\":720,\"height\":1280,\"bitrate\":1961035,\"a_first_pkg_dts\":-115,"
                + "\"v_first_pkg_dts\":-135,\"transcoder_ver\":\"3d0e_atlas-5.5.3\",\"stream_info\":\"AudioVideo\","
                + "\"a_bitrate\":62884,\"a_profile\":\"N\\/A\",\"input_format\":\"mov,mp4,m4a,3gp,3g2,mj2\","
                + "\"color_space\":\"AVCOL_SPC_UNSPECIFIED\",\"v_alpha_type\":0},\"rt_stat\":{\"alive_cnt\":3,"
                + "\"session_id\":175,\"session_uuid\":\"329793xp4lnsok7ob6qesh1Uc0TSM3uS\",\"last_error\":0,"
                + "\"cache_used\":1,\"spb_used\":true,\"spb_th\":1500,\"spb_max_ms\":1000,"
                + "\"buffer_ms_when_play\":3008,\"a_packet_cnt_when_play\":66,\"v_packet_cnt_when_play\":89,"
                + "\"played_dur\":1169,\"actual_played_dur\":1166,\"a_actual_played_dur\":1371,\"alive_dur\":2307,"
                + "\"used_alive_dur\":1169,\"block_cnt\":0,\"dup_block_cnt\":0,\"block_dur\":0,"
                + "\"block_cnt_start_period\":0,\"block_dur_start_period\":0,\"v_dec_dropped_cnt_start_period\":0,"
                + "\"block_cnt_seek_period\":0,\"block_dur_seek_period\":0,\"dropped_dur\":0,\"v_read_cnt\":56,"
                + "\"v_dec_cnt\":35,\"v_render_cnt\":32,\"v_render_dur\":1081,\"diff_render_dur\":290,"
                + "\"v_decoded_dropped_cnt\":0,\"v_render_dropped_cnt\":0,\"v_render_fail_cnt_duration_nan\":3,"
                + "\"a_read_dur\":1932,\"a_dec_dur\":1772,\"a_render_cnt\":30,\"a_render_dur\":1371,"
                + "\"a_silence_cnt\":1,\"a_callback_max_gap\":77,\"hw_dec_err_cnt\":0,\"pixel_format\":\"N\\/A\","
                + "\"loop_cnt\":0,\"avg_fps\":27.373823165893555,\"v_hw_dec\":true,\"a_device_latency\":171,"
                + "\"a_device_applied_latency\":342,\"enable_av_sync_opt2\":1,\"enable_av_sync_opt4\":1,"
                + "\"v_fallback_ffmpeg_cnt\":0,\"v_cur_consecutive_dropped_frame_block_cnt\":0,\"url_cnt\":3,"
                + "\"max_av_diff\":9,\"min_av_diff\":-74,\"dur_out_of_sync\":0,\"dur_out_of_sync_3s\":0,"
                + "\"a_cache_duration\":8244,\"a_cache_packets\":178,\"a_cache_bytes\":90181,"
                + "\"v_cache_duration\":8349,\"v_cache_packets\":247,\"a_read_pkt_dur\":10053,"
                + "\"v_read_pkt_dur\":10200,\"v_cache_bytes\":2007079,\"a_max_dts_diff\":47,\"v_max_dts_diff\":34,"
                + "\"input_av_diff\":34,\"v_render_type\":2000,\"last_pos\":1039,\"use_seek_continuous\":0},"
                + "\"rep_stat\":{\"reps\":[{\"id\":1,\"width\":720,\"height\":1280,\"quality_score\":6000,"
                + "\"max_bitrate\":3396,\"avg_bitrate\":1961,\"dur\":1081,\"kvq_fr\":-1,\"kvq_nr\":-1,"
                + "\"disable\":false,\"type\":\"ohd\",\"tt\":\"ohd37\"},{\"id\":2,\"width\":540,\"height\":960,"
                + "\"quality_score\":5000,\"max_bitrate\":782,\"avg_bitrate\":797,\"dur\":0,\"kvq_fr\":-1,"
                + "\"kvq_nr\":-1,\"disable\":false,\"type\":\"omd\",\"tt\":\"omd21\"},{\"id\":3,\"width\":464,"
                + "\"height\":824,\"quality_score\":3000,\"max_bitrate\":422,\"avg_bitrate\":429,\"dur\":0,"
                + "\"kvq_fr\":-1,\"kvq_nr\":-1,\"disable\":false,\"type\":\"osd\",\"tt\":\"osd21\"}]},"
                + "\"rt_cost\":{\"http_connect\":-1,\"http_first_data\":-1,\"dns_analyze\":-1,\"prepare\":89,"
                + "\"first_screen\":165,\"second_screen\":67,\"total_first_screen\":1148,"
                + "\"start_to_first_screen\":10,\"render_ready\":94,\"start_play_block\":5,\"fst_app_pause\":0,"
                + "\"app_start_play\":1138,\"step\":{\"vod_adaptive_select\":1,\"input_open\":9,"
                + "\"find_stream_info\":5,\"pre_demuxed\":0,\"dec_opened\":73,\"all_prepared\":1,"
                + "\"wait_for_play\":983,\"fst_v_pkt_recv\":75,\"fst_v_pkt_pre_dec\":14,\"fst_v_pkt_dec\":53,"
                + "\"fst_v_render\":10,\"fst_a_pkt_recv\":75,\"fst_a_pkt_pre_dec\":7,\"fst_a_pkt_dec\":2,"
                + "\"fst_a_render\":3,\"select_rep\":-1,\"wait_for_surface\":-1,\"create_dummy_surface\":-1}},"
                + "\"ac_cache\":{\"cfg_cache_key\":\"54ea8ac719d9d99615a948139da48817\",\"data_source_type\":5,"
                + "\"max_cache_bytes\":104857600,\"total_bytes\":7365403,\"cached_bytes\":2621440,"
                + "\"total_cdn_bytes\":1640940,\"total_cdn_cost_ms\":1162,\"first_scope_hit\":true,"
                + "\"scope_open_cnt\":5,\"scope_hit_cnt\":2,\"adapter_error\":0,\"sub_error_code\":0,"
                + "\"scope_max_size_kb_of_settting\":1024,\"scope_max_size_kb_of_cache_content\":512,"
                + "\"con_timeout_ms\":3000,\"read_timeout_ms\":15000,\"socket_buf_size_kb\":256,"
                + "\"scope_size_kb\":524288,\"runloop_cnt\":15,\"http_version\":\"http\\/2+quic\\/46\","
                + "\"downloaded\":false,\"upstream_type\":4,\"cached_bytes_on_open\":980500,\"http_resp_code\":206,"
                + "\"hodor_pos\":5,\"p2sp_init\":true,\"p2sp_enabled\":false,\"p2sp_disabled\":false},"
                + "\"preload_v3\":{\"has_bt_reached\":true,\"bc_bt_reached\":0,\"bc_bt_never_reached\":0,\"t_cnt\":4,"
                + "\"l_cf_cnt\":2,\"s_cf_cnt\":0,\"detail\":[{\"idx\":1,\"l\":1},{\"idx\":4,\"l\":1}]},"
                + "\"vod_adaptive\":{\"adapt_type\":0,\"rep_id\":0,\"rep_order\":3,\"max_rate\":3396,"
                + "\"avg_rate\":1961,\"bitrate_stdev\":0,\"device_width\":720,\"device_height\":1452,\"quality\":6,"
                + "\"fps\":29.58329963684082,\"low_device\":0,\"switch_code\":-100,"
                + "\"representations\":\"[{\\\"rep_id\\\":0,\\\"width\\\":720,\\\"height\\\":1280,\\\"quality\\\":6,"
                + "\\\"fps\\\":29.58329963684082,\\\"avg_bitrate\\\":1961,\\\"adj_avg_bitrate\\\":1961,"
                + "\\\"max_bitrate\\\":3396,\\\"brt_stdev\\\":0,\\\"max_pre_brt\\\":0,"
                + "\\\"quality_type\\\":\\\"ohd\\\",\\\"transcode_type\\\":\\\"_ohd37_\\\",\\\"disable\\\":0,"
                + "\\\"cached\\\":980500},{\\\"rep_id\\\":1,\\\"width\\\":540,\\\"height\\\":960,\\\"quality\\\":5,"
                + "\\\"fps\\\":29.58329963684082,\\\"avg_bitrate\\\":797,\\\"adj_avg_bitrate\\\":797,"
                + "\\\"max_bitrate\\\":782,\\\"brt_stdev\\\":0,\\\"max_pre_brt\\\":0,"
                + "\\\"quality_type\\\":\\\"omd\\\",\\\"transcode_type\\\":\\\"_omd21_\\\",\\\"disable\\\":0,"
                + "\\\"cached\\\":0},{\\\"rep_id\\\":2,\\\"width\\\":464,\\\"height\\\":824,\\\"quality\\\":3,"
                + "\\\"fps\\\":20,\\\"avg_bitrate\\\":429,\\\"adj_avg_bitrate\\\":429,\\\"max_bitrate\\\":422,"
                + "\\\"brt_stdev\\\":0,\\\"max_pre_brt\\\":0,\\\"quality_type\\\":\\\"osd\\\","
                + "\\\"transcode_type\\\":\\\"_osd21_\\\",\\\"disable\\\":0,\\\"cached\\\":0}]\","
                + "\"quality_type\":\"ohd\",\"idle_time\":649,\"dl_idle_time\":789,\"short_bw\":9058,"
                + "\"long_bw\":9058,\"rt_bw\":11397,\"est_real_bw\":0,\"percentile_bw\":0,\"fin_pre_bw\":8967,"
                + "\"bwd_stdev\":10034,\"switch_reason\":\"LowDeviceResolution_WIFI_adap with qua, res and brt\","
                + "\"dl_time\":1162,\"low_res_auto\":1,\"amend_1080p\":0.800000011920929,\"under_wifi\":1,"
                + "\"manifest_type\":1,\"cached\":0,\"width\":720,\"height\":1280,\"net_type\":\"WIFI\","
                + "\"under_5G\":1,\"preload_thr\":8388610,\"preload_s\":0,\"high_device_res\":0,\"nettype_score\":0,"
                + "\"hdr_type\":\"sdr\",\"bwd_amend_1080p\":0,\"bwd_amend_preload\":0,\"fullcache_check\":0,"
                + "\"avg_rtt\":0,\"real_rtt\":0,\"lost_rate\":0,\"thermal_state\":0,\"thermal_state_thresh\":2,"
                + "\"last_two_brt_amend\":0,\"use_brt_ptr\":0,\"bw_compute_process\":\"final_bwd=9058[short bwd] *0"
                + ".900[WIFI]*1.100[bm]=8967kbps\",\"joint_strategy\":0,\"rate_tuning\":\"\","
                + "\"rate_tuning_wifi\":\"\",\"bw_info_queue\":\"\",\"rebuf_ratio\":3.1746034622192383,"
                + "\"kvq_fr_score\":0,\"kvq_nr_score\":0,\"wifi_amend\":0.8999999761581421,\"4G_amend\":0"
                + ".03999999910593033,\"5G_amend\":0.30000001192092896},\"exp_dcc\":{\"used\":true,"
                + "\"dcc_pre_read_ms_abjust\":8230,\"act_mb_ratio\":3.193211555480957,\"dcc_opt\":false,"
                + "\"init_buffer\":8230,\"buffer_low_ratio_th\":0.949999988079071,\"is_unified_dcc_used\":true,"
                + "\"last_max_buffer\":8230},\"sys_prof\":{\"memory\":398540,\"cpu\":24,\"cpu_cnt\":8},"
                + "\"hw_decode\":{\"mediacodec\":{\"input_err_cnt\":15,\"input_err\":-2,"
                + "\"output_try_again_err_cnt\":6,\"config_type\":\"h264h265\",\"max_cnt\":3,\"use_byte_buffer\":0}},"
                + "\"battery_info\":{\"battery_level\":31,\"battery_temperature\":373},\"thermal_state\":3,"
                + "\"server_predicted_watch_time_ms\":-1,\"app_predicted_watch_time_ms\":-1,"
                + "\"interface_cost\":{\"prepare\":4,\"start\":0,\"pause\":-1,\"stop\":-1},"
                + "\"exit_cost\":{\"async\":0,\"read\":-1,\"v_component\":-1,\"a_component\":-1,\"stream_close\":-1},"
                + "\"KPMID\":{\"kernalCreate\":0,\"createType\":\"0\",\"createIndex\":\"1\",\"autoStart\":\"false\","
                + "\"prepareAsync\":4,\"has_do_prefetch\":\"true\",\"do_prefetch_time\":\"9047\","
                + "\"attachVideoOut\":25,\"prepared\":135,\"start\":1143,\"rendered\":1168},"
                + "\"MidWareType\":\"Wayne\",\"MWInfo\":{\"isPreload\":\"false\"}}";
        ObjectMapper mapper = new ObjectMapper();
        JsonParser parser = mapper.createParser(str);
        while (parser.nextToken() != null) {
            System.out.println(parser.currentToken());
            System.out.println(parser.getCurrentLocation().getCharOffset());
            System.out.println(parser.getCurrentLocation().getColumnNr());
        }
    }
}
