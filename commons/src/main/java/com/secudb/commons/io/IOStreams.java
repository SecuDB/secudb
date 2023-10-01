package com.secudb.commons.io;

import java.io.*;
import java.nio.charset.StandardCharsets;

public final class IOStreams {
    public static final int DEFAULT_BYTE_BUFFER_SIZE = 8192;

    private IOStreams() { }

    public static byte[] newByteArrayBuffer() {
        return new byte[DEFAULT_BYTE_BUFFER_SIZE];
    }

    public static void transfer(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = newByteArrayBuffer();
        int read;
        while ((read = in.read(buffer, 0, buffer.length)) >= 0) {
            out.write(buffer, 0, read);
        }
    }

    public static boolean equals(InputStream inputStream1, InputStream inputStream2) throws IOException {
        return compare(inputStream1, inputStream2) == 0;
    }

    public static int compare(InputStream inputStream1, InputStream inputStream2) throws IOException {
        while (true) {
            int b1 = inputStream1.read();
            int b2 = inputStream2.read();
            if (b1 == -1 && b2 == -1) {
                return 0;
            }
            if (b1 == -1) {
                return -1;
            }
            if (b2 == -1) {
                return 1;
            }
            if (b1 != b2) {
                return b1 - b2;
            }
        }
    }

    public static byte[] readExactly(InputStream inputStream, int length) throws IOException {
        int offset = 0;
        byte[] target = new byte[length];
        int r;
        while ((r = inputStream.read(target, offset, length - offset)) > 0) {
            offset += r;
        }
        if (offset != length) {
            throw new EOFException();
        }
        return target;
    }

    public static byte[] readFully(InputStream inputStream, int expectedLength) throws IOException {
        byte[] data = readExactly(inputStream, expectedLength);
        if (inputStream.read() != -1) {
            throw new IOException("Stream is longer than expected");
        }
        return data;
    }

    public static byte[] readFully(InputStream inputStream) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            transfer(inputStream, baos);
            return baos.toByteArray();
        }
    }

    public static void skipExactly(InputStream inputStream, long n) throws IOException {
        long left = n;
        while (left > 0) {
            long skipped = inputStream.skip(n);
            if (skipped < 0) {
                throw new IllegalStateException("Unable to properly skip");
            }
            else if (skipped == 0) {
                if (inputStream.read() == -1) {
                    throw new EOFException();
                }
                left--;
            } else {
                left -= skipped;
            }
        }

        if (left < 0) {
            throw new IllegalStateException("Unable to properly skip");
        }
    }

    public static byte readByte(InputStream inputStream) throws IOException {
        return (byte) readByteAsInt(inputStream);
    }

    public static int readByteAsInt(InputStream inputStream) throws IOException {
        int r = inputStream.read();
        if (r < 0) {
            throw new EOFException();
        }
        return r;
    }

    public static void writeShort(OutputStream out, short value) throws IOException {
        out.write((byte)(value >> 8));
        out.write((byte)(value >> 0));
    }

    public static short readShort(InputStream in) throws IOException {
        int b1 = in.read();
        if (b1 < 0) {
            throw new EOFException();
        }
        int b2 = in.read();
        if (b2 < 0) {
            throw new EOFException();
        }
        return (short)((b1 << 8) + (b2 << 0));
    }
}
