package com.kuaishou.data.udf.video;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by hanzhiheng on 2017/8/11.
 * hive 高版本中md5函数官方实现
 * 搬运工
 */
@Description(name = "md5",
		value = "_FUNC_(str or bin) - Calculates an MD5 128-bit checksum for the string or binary.",
		extended = "The value is returned as a string of 32 hex digits, or NULL if the argument was NULL.\n"
				+ "Example:\n"
				+ "  > SELECT _FUNC_('ABC');\n"
				+ "  '902fbdd2b1df0c4f70b4a5d23525e932'\n"
				+ "  > SELECT _FUNC_(binary('ABC'));\n"
				+ "  '902fbdd2b1df0c4f70b4a5d23525e932'")
public class Md5UDF extends UDF{
	private final Text result = new Text();
	private final MessageDigest digest;

	public Md5UDF() {
		try {
			digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Convert String to md5
	 */
	public Text evaluate(Text n) {
		if (n == null) {
			return null;
		}

		digest.reset();
		digest.update(n.getBytes(), 0, n.getLength());
		byte[] md5Bytes = digest.digest();
		String md5Hex = Hex.encodeHexString(md5Bytes);

		result.set(md5Hex);
		return result;
	}

	/**
	 * Convert bytes to md5
	 */
	public Text evaluate(BytesWritable b) {
		if (b == null) {
			return null;
		}

		digest.reset();
		digest.update(b.getBytes(), 0, b.getLength());
		byte[] md5Bytes = digest.digest();
		String md5Hex = Hex.encodeHexString(md5Bytes);

		result.set(md5Hex);
		return result;
	}

	public static void main(String[] args) {
		Md5UDF md5UDF = new Md5UDF();
		Text str = new Text("A000004F88C2B2");
		Text str_md5 = new Text("4d94e64325a0d9b10753ed599dbcee96");
		System.out.println(md5UDF.evaluate(str));
		System.out.println(md5UDF.evaluate(str).equals(str_md5));
	}

}
