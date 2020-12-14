package com.kuaishou.data.udf.video.utils;

import java.nio.ByteBuffer;

/**
 * @author wuxuexin <wuxuexin@kuaishou.com>
 * Created on 2020-12-14
 */
public class CityHash {
    private static final long k0 = -4348849565147123417L;
    private static final long k1 = -5435081209227447693L;
    private static final long k2 = -7286425919675154353L;
    private static final long k3 = -3942382747735136937L;
    private static final long kMul = -7070675565921424023L;

    public CityHash() {
    }

    private static long toLongLE(byte[] b, int i) {
        return ((long) b[i + 7] << 56) + ((long) (b[i + 6] & 255) << 48) + ((long) (b[i + 5] & 255) << 40) + ((long) (b[i + 4] & 255) << 32) + ((long) (b[i + 3] & 255) << 24) + (long) ((b[i + 2] & 255) << 16) + (long) ((b[i + 1] & 255) << 8) + (long) ((b[i + 0] & 255) << 0);
    }

    private static int toIntLE(byte[] b, int i) {
        return ((b[i + 3] & 255) << 24) + ((b[i + 2] & 255) << 16) + ((b[i + 1] & 255) << 8) + ((b[i + 0] & 255) << 0);
    }

    private static long fetch64(byte[] s, int pos) {
        return toLongLE(s, pos);
    }

    private static int fetch32(byte[] s, int pos) {
        return toIntLE(s, pos);
    }

    private static long rotate(long val, int shift) {
        return shift == 0 ? val : val >>> shift | val << 64 - shift;
    }

    private static long rotateByAtLeast1(long val, int shift) {
        return val >>> shift | val << 64 - shift;
    }

    private static long shiftMix(long val) {
        return val ^ val >>> 47;
    }

    private static long hash128to64(long u, long v) {
        long a = (u ^ v) * -7070675565921424023L;
        a ^= a >>> 47;
        long b = (v ^ a) * -7070675565921424023L;
        b ^= b >>> 47;
        b *= -7070675565921424023L;
        return b;
    }

    private static long hashLen16(long u, long v) {
        return hash128to64(u, v);
    }

    private static long hashLen0to16(byte[] s, int pos, int len) {
        if (len > 8) {
            long a = fetch64(s, pos + 0);
            long b = fetch64(s, pos + len - 8);
            return hashLen16(a, rotateByAtLeast1(b + (long) len, len)) ^ b;
        } else if (len >= 4) {
            long a = 4294967295L & (long) fetch32(s, pos + 0);
            return hashLen16((a << 3) + (long) len, 4294967295L & (long) fetch32(s, pos + len - 4));
        } else if (len > 0) {
            int a = s[pos + 0] & 255;
            int b = s[pos + (len >>> 1)] & 255;
            int c = s[pos + len - 1] & 255;
            int y = a + (b << 8);
            int z = len + (c << 2);
            return shiftMix((long) y * -7286425919675154353L ^ (long) z * -3942382747735136937L) * -7286425919675154353L;
        } else {
            return -7286425919675154353L;
        }
    }

    private static long hashLen17to32(byte[] s, int pos, int len) {
        long a = fetch64(s, pos + 0) * -5435081209227447693L;
        long b = fetch64(s, pos + 8);
        long c = fetch64(s, pos + len - 8) * -7286425919675154353L;
        long d = fetch64(s, pos + len - 16) * -4348849565147123417L;
        return hashLen16(rotate(a - b, 43) + rotate(c, 30) + d, a + rotate(b ^ -3942382747735136937L, 20) - c + (long) len);
    }

    private static long[] weakHashLen32WithSeeds(long w, long x, long y, long z, long a, long b) {
        a += w;
        b = rotate(b + a + z, 21);
        long c = a;
        a += x;
        a += y;
        b += rotate(a, 44);
        return new long[]{a + z, b + c};
    }

    private static long[] weakHashLen32WithSeeds(byte[] s, int pos, long a, long b) {
        return weakHashLen32WithSeeds(fetch64(s, pos + 0), fetch64(s, pos + 8), fetch64(s, pos + 16), fetch64(s, pos + 24), a, b);
    }

    private static long hashLen33to64(byte[] s, int pos, int len) {
        long z = fetch64(s, pos + 24);
        long a = fetch64(s, pos + 0) + (fetch64(s, pos + len - 16) + (long) len) * -4348849565147123417L;
        long b = rotate(a + z, 52);
        long c = rotate(a, 37);
        a += fetch64(s, pos + 8);
        c += rotate(a, 7);
        a += fetch64(s, pos + 16);
        long vf = a + z;
        long vs = b + rotate(a, 31) + c;
        a = fetch64(s, pos + 16) + fetch64(s, pos + len - 32);
        z = fetch64(s, pos + len - 8);
        b = rotate(a + z, 52);
        c = rotate(a, 37);
        a += fetch64(s, pos + len - 24);
        c += rotate(a, 7);
        a += fetch64(s, pos + len - 16);
        long wf = a + z;
        long ws = b + rotate(a, 31) + c;
        long r = shiftMix((vf + ws) * -7286425919675154353L + (wf + vs) * -4348849565147123417L);
        return shiftMix(r * -4348849565147123417L + vs) * -7286425919675154353L;
    }

    private static long cityHash64(byte[] s, int pos, int len) {
        if (len <= 32) {
            return len <= 16 ? hashLen0to16(s, pos, len) : hashLen17to32(s, pos, len);
        } else if (len <= 64) {
            return hashLen33to64(s, pos, len);
        } else {
            long x = fetch64(s, pos + len - 40);
            long y = fetch64(s, pos + len - 16) + fetch64(s, pos + len - 56);
            long z = hashLen16(fetch64(s, pos + len - 48) + (long) len, fetch64(s, pos + len - 24));
            long[] v = weakHashLen32WithSeeds(s, pos + len - 64, (long) len, z);
            long[] w = weakHashLen32WithSeeds(s, pos + len - 32, y + -5435081209227447693L, x);
            x = x * -5435081209227447693L + fetch64(s, pos + 0);
            len = len - 1 & -64;

            long swap;
            do {
                x = rotate(x + y + v[0] + fetch64(s, pos + 8), 37) * -5435081209227447693L;
                y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * -5435081209227447693L;
                x ^= w[1];
                y += v[0] + fetch64(s, pos + 40);
                z = rotate(z + w[0], 33) * -5435081209227447693L;
                v = weakHashLen32WithSeeds(s, pos + 0, v[1] * -5435081209227447693L, x + w[0]);
                w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y + fetch64(s, pos + 16));
                swap = z;
                z = x;
                x = swap;
                pos += 64;
                len -= 64;
            } while (len != 0);

            return hashLen16(hashLen16(v[0], w[0]) + shiftMix(y) * -5435081209227447693L + z, hashLen16(v[1], w[1]) + swap);
        }
    }

    public static long cityHash64WithSeed(long s, long seed) {
        return cityHash64WithSeed(ByteBuffer.allocate(8).putLong(s).array(), 0, 8, seed);
    }

    private static long cityHash64WithSeed(byte[] s, int pos, int len, long seed) {
        return cityHash64WithSeeds(s, pos, len, -7286425919675154353L, seed);
    }

    private static long cityHash64WithSeeds(byte[] s, int pos, int len, long seed0, long seed1) {
        return hashLen16(cityHash64(s, pos, len) - seed0, seed1);
    }

    private static long[] cityMurmur(byte[] s, int pos, int len, long seed0, long seed1) {
        long b = seed1;
        long c = 0L;
        long d = 0L;
        int l = len - 16;
        long a;
        if (l <= 0) {
            a = shiftMix(seed0 * -5435081209227447693L) * -5435081209227447693L;
            c = seed1 * -5435081209227447693L + hashLen0to16(s, pos, len);
            d = shiftMix(a + (len >= 8 ? fetch64(s, pos + 0) : c));
        } else {
            c = hashLen16(fetch64(s, pos + len - 8) + -5435081209227447693L, seed0);
            d = hashLen16(seed1 + (long) len, c + fetch64(s, pos + len - 16));
            a = seed0 + d;

            do {
                a ^= shiftMix(fetch64(s, pos + 0) * -5435081209227447693L) * -5435081209227447693L;
                a *= -5435081209227447693L;
                b ^= a;
                c ^= shiftMix(fetch64(s, pos + 8) * -5435081209227447693L) * -5435081209227447693L;
                c *= -5435081209227447693L;
                d ^= c;
                pos += 16;
                l -= 16;
            } while (l > 0);
        }

        a = hashLen16(a, c);
        b = hashLen16(d, b);
        return new long[]{a ^ b, hashLen16(b, a)};
    }

    private static long[] cityHash128WithSeed(byte[] s, int pos, int len, long seed0, long seed1) {
        if (len < 128) {
            return cityMurmur(s, pos, len, seed0, seed1);
        } else {
            long[] v = new long[2];
            long[] w = new long[2];
            long x = seed0;
            long y = seed1;
            long z = -5435081209227447693L * (long) len;
            v[0] = rotate(seed1 ^ -5435081209227447693L, 49) * -5435081209227447693L + fetch64(s, pos);
            v[1] = rotate(v[0], 42) * -5435081209227447693L + fetch64(s, pos + 8);
            w[0] = rotate(seed1 + z, 35) * -5435081209227447693L + seed0;
            w[1] = rotate(seed0 + fetch64(s, pos + 88), 53) * -5435081209227447693L;

            long swap;
            do {
                x = rotate(x + y + v[0] + fetch64(s, pos + 8), 37) * -5435081209227447693L;
                y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * -5435081209227447693L;
                x ^= w[1];
                y += v[0] + fetch64(s, pos + 40);
                z = rotate(z + w[0], 33) * -5435081209227447693L;
                v = weakHashLen32WithSeeds(s, pos + 0, v[1] * -5435081209227447693L, x + w[0]);
                w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y + fetch64(s, pos + 16));
                swap = z;
                z = x;
                pos += 64;
                x = rotate(swap + y + v[0] + fetch64(s, pos + 8), 37) * -5435081209227447693L;
                y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * -5435081209227447693L;
                x ^= w[1];
                y += v[0] + fetch64(s, pos + 40);
                z = rotate(z + w[0], 33) * -5435081209227447693L;
                v = weakHashLen32WithSeeds(s, pos, v[1] * -5435081209227447693L, x + w[0]);
                w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y + fetch64(s, pos + 16));
                swap = z;
                z = x;
                x = swap;
                pos += 64;
                len -= 128;
            } while (len >= 128);

            x = swap + rotate(v[0] + z, 49) * -4348849565147123417L;
            z += rotate(w[0], 37) * -4348849565147123417L;

            for (int tail_done = 0; tail_done < len; v = weakHashLen32WithSeeds(s, pos + len - tail_done, v[0] + z, v[1])) {
                tail_done += 32;
                y = rotate(x + y, 42) * -4348849565147123417L + v[1];
                w[0] += fetch64(s, pos + len - tail_done + 16);
                x = x * -4348849565147123417L + w[0];
                z += w[1] + fetch64(s, pos + len - tail_done);
                w[1] += v[0];
            }

            x = hashLen16(x, v[0]);
            y = hashLen16(y + z, w[0]);
            return new long[]{hashLen16(x + v[1], w[1]) + y, hashLen16(x + w[1], y + v[1])};
        }
    }
}
