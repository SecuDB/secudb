package com.secudb.commons.io;

import java.nio.ByteBuffer;

public final class Bytes {
    private Bytes() { }


    public static byte[] clone(byte[] data) {
        return clone(data, 0, data.length);
    }

    public static byte[] clone(byte[] data, int offset) {
        return clone(data, offset, data.length - offset);
    }

    public static byte[] clone(byte[] data, int offset, int length) {
        byte[] duplicated = new byte[length];
        System.arraycopy(data, offset, duplicated, 0, length);
        return duplicated;
    }


    public static byte[] toByteArray(byte value) {
        return new byte[] { value };
    }

    public static byte[] toByteArray(short value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Short.BYTES);
        byteBuffer.putShort(value);
        return byteBuffer.array();
    }

    public static byte[] toByteArray(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(value);
        return byteBuffer.array();
    }

    public static byte[] toByteArray(long value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
        byteBuffer.putLong(value);
        return byteBuffer.array();
    }

    public static int toShort(byte[] data) {
        return toShort(data, 0);
    }

    public static short toShort(byte[] data, int offset) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Short.BYTES);
        byteBuffer.put(data, offset, Short.BYTES);
        byteBuffer.rewind();
        return byteBuffer.getShort();
    }

    public static int toInteger(byte[] data) {
        return toInteger(data, 0);
    }

    public static int toInteger(byte[] data, int offset) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.put(data, offset, Integer.BYTES);
        byteBuffer.rewind();
        return byteBuffer.getInt();
    }

    public static long toLong(byte[] data) {
        return toLong(data, 0);
    }

    public static long toLong(byte[] data, int offset) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
        byteBuffer.put(data, offset, Long.BYTES);
        byteBuffer.rewind();
        return byteBuffer.getLong();
    }
}
