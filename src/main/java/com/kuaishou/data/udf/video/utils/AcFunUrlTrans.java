package com.kuaishou.data.udf.video.utils;

import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.codec.binary.Base64;

import org.apache.hadoop.shaded.org.apache.commons.io.FilenameUtils;

import com.google.protobuf.InvalidProtocolBufferException;
import com.kuaishou.data.udf.video.protobuf.FileName;
import com.kuaishou.keycenter.security.aes.AESCBCCoder;
import com.kuaishou.keycenter.security.aes.AESCBCFixedIVCoder;

/**
 * @author <a href="mailto:guoyu03@kuaishou.com">Guo Yu</a>
 */
public class AcFunUrlTrans {

    private static final byte[] AES_KEY_BYTES = new byte[] {
            0x06, 0x7d, 0x1a, 0x44, (byte) 0x71, 0x4c, 0x51, (byte) 0x83,
            0x35, (byte) 0xdf, 0x13, (byte) 0xde, 0x4e, 0x1b, 0x49, (byte) 0xaf};

    private static final byte[] AES_IV_BYTES = {
            (byte) 0xa2, (byte) 0xc8, 0x7c, (byte) 0xe4, (byte) 0xeb, (byte) 0xbe, 0x43, 0x1b,
            0x72, 0x49, 0x54, 0x72, 0x43, 0x4a, (byte) 0xde, 0x078};

    private static final AESCBCCoder CODER = AESCBCFixedIVCoder.of(AES_KEY_BYTES, AES_IV_BYTES);

    public static String generateName(long taskId) {
        return generateName(taskId, "");
    }

    public static String generateName(long taskId, String subTask) {
        FileName name = FileName.newBuilder()
                .setTaskId(taskId).setSubTask(subTask)
                .setTimestamp(ThreadLocalRandom.current().nextLong()).build();
        try {
            byte[] cipherBytes = CODER.encrypt(name.toByteArray());
            return Base64.encodeBase64URLSafeString(cipherBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static FileName parseName(String fileNameStr) {
        try {
            byte[] cipherBytes = Base64.decodeBase64(fileNameStr);
            byte[] nameBytes = CODER.decrypt(cipherBytes);
            return FileName.parseFrom(nameBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws InvalidProtocolBufferException {
        System.out.println(parseSubTaskId("http://ali-video.acfun.cn/mediacloud/acfun/acfun_video/segment" +
                "/nMTtidUZitlB_14ckXvSusm6xJrAlFJ5zoe9IcxP61bKypXevNaZOPV6pDj2WMnY.m3u8"));
        System.out.println(parseSubTaskId(
                "http://36.158.208.226/030001100057BDD47F47C02D9B7D2FCAAAA6CA- AF17-249B-7CF1-0B1BB6E3C7D2.m3u8"));
        System.out.println(parseSubTaskId("http://ali-video.acfun.cn/mediacloud/acfun/acfun_video/segment" +
                "/192568069023866880_HLS_720P_HEVC_2.m3u8"));
        URI uri = URI.create(
                "http://ali-video.acfun.cn/mediacloud/acfun/acfun_video/segment/192568069023866880_HLS_720P_HEVC_2"
                        + ".m3u8");

        System.out.println(uri.getPath());
    }

    public static String parseSubTask(String url) throws InvalidProtocolBufferException {
        FileName fileName = null;
        try {
            if (!url.startsWith("http")) {
                return null;
            }
            String path = URI.create(url).getPath();
            if (!path.endsWith(".m3u8")) {
                return null;
            }
            String encryptedName = FilenameUtils.getBaseName(path);
            if (encryptedName.matches("\\d{15}.+")) {
                return encryptedName.substring(encryptedName.indexOf("_") + 1);
            }
            fileName = parseName(encryptedName);
        } catch (Exception e) {
            return null;
        }
        return fileName == null ? null : fileName.getSubTask();
    }

    public static String parseSubTaskId(String url) throws InvalidProtocolBufferException {
        FileName fileName = null;
        try {
            if (!url.startsWith("http")) {
                return null;
            }
            String path = URI.create(url).getPath();
            if (!path.endsWith(".m3u8")) {
                return null;
            }
            String encryptedName = FilenameUtils.getBaseName(path);
            if (encryptedName.matches("\\d{15}.+")) {
                return encryptedName.substring(0, encryptedName.indexOf("_"));
            }
            fileName = parseName(encryptedName);
        } catch (Exception e) {
            return null;
        }
        return fileName == null ? null : String.valueOf(fileName.getTaskId());
    }

}
