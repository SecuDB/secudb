package com.secudb.commons.binary;

import com.secudb.commons.hash.Hash;

import java.nio.ByteBuffer;

public class ByteArrayBinary extends AbstractBinary {
    private byte[] source;
    private int offset;
    private int length;

    public ByteArrayBinary(byte[] source) {
        this.source = source;
        this.offset = 0;
        this.length = source.length;
    }

    public ByteArrayBinary(byte[] source, int offset, int length) {
        this.source = source;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public boolean hasLength() {
        return true;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public boolean isSingleReadStream() {
        return false;
    }

    @Override
    public byte[] getBytes() {
        byte[] src = getBytesReadOnly();
        byte[] dest = new byte[src.length];
        System.arraycopy(src, 0, dest, 0, src.length);
        return dest;
    }

    @Override
    public byte[] getBytesReadOnly() {
        if (offset == 0 && length == source.length) {
            return source;
        }

        synchronized (this) {
            byte[] simplified = new byte[length];
            System.arraycopy(source, offset, simplified, 0, length);
            source = simplified;
            offset = 0;
        }
        return source;
    }

    @Override
    public Binary fragment(long offset, long length) {
        if (length == 0) return EMPTY;
        int len = (int)length;
        if (length == -1) {
            len = this.length - (int)offset;
        }
        return new ByteArrayBinary(source, this.offset + (int)offset, len);
    }

    @Override
    public boolean isInMemory() {
        return true;
    }

    @Override
    public Binary toMemory() {
        return this;
    }

    @Override
    public ByteBuffer toByteBufferReadOnly(boolean direct) {
        if (!direct) {
            return ByteBuffer.wrap(source, offset, length);
        }

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(length);
        toByteBuffer(byteBuffer);
        byteBuffer.flip();
        return byteBuffer;
    }

    @Override
    public void toByteBuffer(ByteBuffer byteBuffer) {
        byteBuffer.put(source, offset, length);
    }

    @Override
    protected byte[] computeHash(Hash hash) {
        return hash.compute(source, offset, length);
    }

}
