package com.vladykin.replicamap.kafka.impl.util;

import com.vladykin.replicamap.ReplicaMapException;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.vladykin.replicamap.kafka.impl.util.Utils.assignPartitionsRoundRobin;
import static com.vladykin.replicamap.kafka.impl.util.Utils.generateUniqueNodeId;
import static com.vladykin.replicamap.kafka.impl.util.Utils.getMacAddresses;
import static com.vladykin.replicamap.kafka.impl.util.Utils.macHash1;
import static com.vladykin.replicamap.kafka.impl.util.Utils.rotateRight;
import static java.lang.Integer.toBinaryString;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class UtilsTest {
    @Test
    void testGenerateUniqueNodeId() {
        long time = System.currentTimeMillis();
        List<byte[]> macs = getMacAddresses();
        SecureRandom rnd = new SecureRandom();

        // 10_000 starts of 3 nodes on the same box at the same time
        for (int i = 0; i < 10_000; i++) {
            HashSet<String> set = new HashSet<>();
            for (int x = 0; x < 3; x++) {
                if (!set.add(Long.toHexString(generateUniqueNodeId(time, macs, rnd))))
                    Assertions.fail("Iteration: " + i + " set: " + set);
            }
        }
    }

    @Test
    void testMacAddresses() {
        List<byte[]> macs = getMacAddresses();
        assertFalse(macs.isEmpty());

        for (byte[] mac : macs)
            assertTrue(mac.length >= 6);
    }

    @Test
    void testRotate() {
        checkRotateRight((byte)0b10010111, 3, (byte)0b11110010);
        checkRotateRight((byte)0b10111000, 3, (byte)0b00010111);
        checkRotateRight((byte)0b10111010, 3, (byte)0b01010111);
        checkRotateRight((byte)0b00000001, 3, (byte)0b00100000);
        checkRotateRight((byte)0b00000001, 4, (byte)0b00010000);
        checkRotateRight((byte)0b00000001, 8, (byte)0b00000001);
        checkRotateRight((byte)0b00000001, 1, (byte)0b10000000);
        checkRotateRight((byte)0b10011001, 2, (byte)0b01100110);
        checkRotateRight((byte)0b00000001, 10, (byte)0b01000000);
        checkRotateRight((byte)0b00000001, 24, (byte)0b00000001);
        checkRotateRight((byte)0b00000001, 27, (byte)0b00100000);
    }

    void checkRotateRight(byte x, int shift, byte exp) {
        byte y = rotateRight(x, shift);

        if (exp != y)
            fail("Expected shift of " + toBinaryString(x) + " is " + toBinaryString(exp) + ", actual " + toBinaryString(y));
    }

    @Test
    void testMacHash1() {
        byte[] x = {1, 2, 3, 4, 5, 6};
        byte[] y = {1, 2, 3, 4, 6, 5};

        assertEquals(macHash1(singletonList(x)), macHash1(singletonList(x)));
        assertEquals(macHash1(singletonList(y)), macHash1(singletonList(y)));
        assertNotEquals(macHash1(singletonList(x)), macHash1(singletonList(y)));
    }

    @Test
    void testMacHash1Collisions() {
        Random rnd = ThreadLocalRandom.current();

        int byteRange = 256;
        int len = 6;
        int attempts = 1000_000;

        int uniqsAll = 0;
        int uniqsMin = Integer.MAX_VALUE;
        int uniqsMax = 0;

        int[] uniqsHistogram = new int[byteRange];

        for (int i = 0; i < attempts; i++) {
            boolean[] set = new boolean[byteRange];
            int uniqs = 0;

            for (;;) {
                byte[] x = new byte[len];
                rnd.nextBytes(x);

                int b = macHash1(singletonList(x)) & 0xFF;

                if (set[b])
                    break;

                set[b] = true;
                uniqs++;
            }

            uniqsHistogram[uniqs] += uniqs;
            uniqsAll += uniqs;

            if (uniqs < uniqsMin)
                uniqsMin = uniqs;

            if (uniqs > uniqsMax)
                uniqsMax = uniqs;
        }

        int uniqsAvg = uniqsAll / attempts;

        System.out.println("min: " + uniqsMin + " avg: " + uniqsAvg + " max: " + uniqsMax);

        double[] uniqsHistogramPercent = new double[byteRange];
        for (int i = 0; i < byteRange; i++)
            uniqsHistogramPercent[i] = uniqsHistogram[i] / (double) uniqsAll * 100;

//        for (int i = 0; i < byteRange; i++)
//            System.out.println(i + " = " + uniqsGistoPercent[i] + " %");

        double clashProbability = 0;

        for (int i = 0; i < 2; i++)
            clashProbability += uniqsHistogramPercent[i];

        System.out.println("clash probability 2: " + clashProbability + " %");
        assertTrue(clashProbability < 0.03);

        for (int i = 2; i < 3; i++)
            clashProbability += uniqsHistogramPercent[i];
        assertTrue(clashProbability < 0.11);

        System.out.println("clash probability 3: " + clashProbability + " %");

        for (int i = 3; i < 5; i++)
            clashProbability += uniqsHistogramPercent[i];

        System.out.println("clash probability 5: " + clashProbability + " %");
        assertTrue(clashProbability < 0.6);

        for (int i = 5; i < 10; i++)
            clashProbability += uniqsHistogramPercent[i];

        System.out.println("clash probability 10: " + clashProbability + " %");
        assertTrue(clashProbability < 5.2);

        for (int i = 10; i < 20; i++)
            clashProbability += uniqsHistogramPercent[i];

        System.out.println("clash probability 20: " + clashProbability + " %");
        assertTrue(clashProbability < 33);

        for (int i = 20; i < 30; i++)
            clashProbability += uniqsHistogramPercent[i];

        System.out.println("clash probability 30: " + clashProbability + " %");

        for (int i = 30; i < 40; i++)
            clashProbability += uniqsHistogramPercent[i];

        System.out.println("clash probability 40: " + clashProbability + " %");

        for (int i = 40; i < 50; i++)
            clashProbability += uniqsHistogramPercent[i];

        System.out.println("clash probability 50: " + clashProbability + " %");

        assertTrue(uniqsAll > 18);
    }

    @Test
    void testCheckPositive() {
        assertThrows(ReplicaMapException.class, () -> Utils.checkPositive(0, "x"));
        assertThrows(ReplicaMapException.class, () -> Utils.checkPositive(-1, "x"));
        Utils.checkPositive(1, "x");
    }

    @Test
    void testAssignPartitionsRoundRobin() {
        assertEquals(new HashSet<>(asList(0, 3, 6)),
            assignPartitionsRoundRobin(0, 3, 8, null));
        assertEquals(new HashSet<>(asList(1, 4, 7)),
            assignPartitionsRoundRobin(1, 3, 8, null));
        assertEquals(new HashSet<>(asList(2, 5)),
            assignPartitionsRoundRobin(2, 3, 8, null));

        assertEquals(new HashSet<>(asList(0, 4, 8)),
            assignPartitionsRoundRobin(0, 4, 12, null));
        assertEquals(new HashSet<>(asList(1, 5, 9)),
            assignPartitionsRoundRobin(1, 4, 12, null));
        assertEquals(new HashSet<>(asList(2, 6, 10)),
            assignPartitionsRoundRobin(2, 4, 12, null));
        assertEquals(new HashSet<>(asList(3, 7, 11)),
            assignPartitionsRoundRobin(3, 4, 12, null));
    }

    @Test
    void testContains() {
        assertFalse(Utils.contains(new short[] {1, 2, 3}, (short)0));
        assertTrue(Utils.contains(new short[] {1, 2, 3}, (short)1));
        assertTrue(Utils.contains(new short[] {1, 2, 3}, (short)2));
        assertTrue(Utils.contains(new short[] {1, 2, 3}, (short)3));
        assertFalse(Utils.contains(new short[] {1, 2, 3}, (short)4));

        short[] arr = new short[100];
        for (int i = 0; i < arr.length; i++)
            arr[i] = (short)(i * 2);

//        System.out.println(Arrays.toString(arr));

        for (int i = 0; i < 101; i++) {
//            System.out.println(i);
            assertEquals((i & 1) == 0, Utils.contains(arr, (short)i));
        }
    }
}
