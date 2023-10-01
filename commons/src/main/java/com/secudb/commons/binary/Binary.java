package com.secudb.commons.binary;

import com.secudb.commons.hash.Hash;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.function.Supplier;

public interface Binary extends Closeable, Supplier<InputStream> {
    Binary EMPTY = new EmptyBinary();

    boolean hasLength();
    long getLength();

    boolean isEmpty();

    boolean isSingleReadStream();

    byte[] getBytes();
    byte[] getBytesReadOnly();

    long writeTo(OutputStream out);
    long writeTo(WritableByteChannel fileChannel);
    long writeTo(FileChannel fileChannel, long position);
    long writeTo(DataOutput out);
    default long writeTo(DataOutputStream out) {
        return writeTo((OutputStream) out);
    }

    Binary fragment(long offset);
    Binary fragment(long offset, long length);

    String toString(Charset charset);
    String toStringUTF8();

    String toBase64();
    String toBase64URL();

    InputStream toInputStream();

    boolean isInMemory();
    Binary toMemory();

    /**
     * Ensure that it could be read multiple times
     *
     * @return
     */
    Binary toRepeatableStream();

    default ByteBuffer toByteBufferReadOnly(boolean direct) {
        byte[] data = getBytesReadOnly();
        ByteBuffer byteBuffer;
        if (direct) {
            byteBuffer = ByteBuffer.allocateDirect(data.length);
        } else {
            byteBuffer = ByteBuffer.allocate(data.length);
        }
        toByteBuffer(byteBuffer);
        byteBuffer.flip();
        return byteBuffer;
    }

    default void toByteBuffer(ByteBuffer byteBuffer) {
        byteBuffer.put(getBytesReadOnly());
    }

    @Override
    default InputStream get() {
        return toInputStream();
    }

    byte[] getHash(Hash hash) throws IOException;

    static Binary fromString(String s, Charset charset) {
        return new ByteArrayBinary(s.getBytes(charset));
    }

    static Binary fromStringUTF8(String s) {
        return fromString(s, StandardCharsets.UTF_8);
    }

    static Binary fromByteArray(byte[] b) {
        return new ByteArrayBinary(b);
    }

    static Binary fromByteArray(byte[] b, int offset, int length) {
        return new ByteArrayBinary(b, offset, length);
    }

    static Binary fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer.hasArray()) {
            return new ByteArrayBinary(byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.remaining());
        }
        byte[] data = new byte[byteBuffer.remaining()];
        byteBuffer.get(data);
        return new ByteArrayBinary(data);
    }

    static Binary fromBase64(String s) {
        return new ByteArrayBinary(Base64.getDecoder().decode(s));
    }

    static Binary fromBase64URL(String s) {
        return new ByteArrayBinary(Base64.getDecoder().decode(s));
    }

    static Binary fromInputStream(InputStream inputStream) {
        return new InputStreamBinary(inputStream);
    }

    static Binary fromInputStream(InputStream inputStream, long length) {
        return new InputStreamBinary(inputStream, length);
    }

    static Binary fromFileChannel(FileChannel fileChannel) {
        return new FileChannelBinary(fileChannel);
    }

    static Binary fromFileChannel(FileChannel fileChannel, long position, long length) {
        return new FileChannelBinary(fileChannel, position, length);
    }

    static Binary fromFile(Path path) {
        return new FileBinary(path);
    }

    static Binary fromFile(Path path, long position, long length) {
        return new FileBinary(path, position, length);
    }

    static Binary fromFile(File file) {
        return new FileBinary(file.toPath());
    }

    static Binary fromFile(File file, long position, long length) {
        return new FileBinary(file.toPath(), position, length);
    }

    static Binary fromValue(byte value) {
        return fromByteArray(new byte[] { value });
    }

    static Binary fromValue(short value) {
        return fromByteArray(new byte[]{
                (byte)(value >>> 8),
                (byte)(value)
        });
    }

    static Binary fromValue(int value) {
        return fromByteArray(new byte[]{
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)(value)
        });
    }

    static Binary fromValue(long value) {
        return fromByteArray(new byte[]{
                (byte)(value >>> 56),
                (byte)(value >>> 48),
                (byte)(value >>> 40),
                (byte)(value >>> 32),
                (byte)(value >>> 24),
                (byte)(value >>> 16),
                (byte)(value >>> 8),
                (byte)(value)
        });
    }

    static Binary fromValue(float value) {
        return fromValue(Float.floatToRawIntBits(value));
    }

    static Binary fromValue(double value) {
        return fromValue(Double.doubleToRawLongBits(value));
    }

    static Binary composite(Binary... binaries) {
        return new CompositeBinary(Arrays.asList(binaries));
    }

    static Binary composite(Collection<Binary> binaries) {
        return new CompositeBinary(binaries);
    }
}
