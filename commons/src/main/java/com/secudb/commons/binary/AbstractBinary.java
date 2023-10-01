package com.secudb.commons.binary;

import com.secudb.commons.hash.Hash;
import com.secudb.commons.io.Bytes;
import com.secudb.commons.io.IOStreams;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;


public abstract class AbstractBinary implements Binary {
    protected int hashCode;

    private Map<String, byte[]> hashes;

    @Override
    public boolean isEmpty() {
        return getLength() == 0;
    }

    @Override
    public byte[] getBytes() {
        try (InputStream in = toInputStream()) {
            return IOStreams.readFully(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public byte[] getBytesReadOnly() {
        return getBytes();
    }

    @Override
    public long writeTo(OutputStream out) {
        byte[] bytes = getBytesReadOnly();
        try {
            out.write(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.length;
    }

    @Override
    public long writeTo(WritableByteChannel channel) {
        byte[] bytes = getBytesReadOnly();
        try {
            channel.write(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.length;
    }

    @Override
    public long writeTo(FileChannel fileChannel, long position) {
        byte[] bytes = getBytesReadOnly();
        try {
            fileChannel.write(ByteBuffer.wrap(bytes), position);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.length;
    }

    @Override
    public long writeTo(DataOutput out) {
        byte[] bytes = getBytesReadOnly();
        try {
            out.write(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.length;
    }

    @Override
    public Binary fragment(long offset) {
        return fragment(offset, -1);
    }

    @Override
    public String toString(Charset charset) {
        return new String(getBytesReadOnly(), charset);
    }

    @Override
    public String toStringUTF8() {
        return toString(StandardCharsets.UTF_8);
    }

    @Override
    public InputStream toInputStream() {
        return new ByteArrayInputStream(getBytesReadOnly());
    }

    @Override
    public Binary toMemory() {
        try (InputStream inputStream = toInputStream()) {
            byte[] data;
            if (hasLength()) {
                data = IOStreams.readFully(inputStream);
            } else {
                if (getLength() > Integer.MAX_VALUE) {
                    throw new IOException("Data too long");
                }
                data = IOStreams.readFully(inputStream, (int)getLength());
            }
            return new ByteArrayBinary(data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Binary toRepeatableStream() {
        if (!isSingleReadStream()) {
            return this;
        }
        return toMemory(); //TODO: make temporary file and manage its deletion later on close()
    }

    private Map<String, byte[]> getHashes() {
        if (hashes == null) {
            hashes = new LinkedHashMap<>();
        }
        return hashes;
    }

    @Override
    public byte[] getHash(Hash hash) throws IOException {
        String algorithmName = hash.getName();
        byte[] hashValue = getHashes().get(algorithmName);
        if (hashValue == null) {
            hashValue = computeHash(hash);
            hashes.put(algorithmName, hashValue);
        }
        return Bytes.clone(hashValue);
    }

    protected void setHash(Hash hash, byte[] value) {
        getHashes().put(hash.getName(), value);
    }

    protected byte[] computeHash(Hash hash) throws IOException {
        try (InputStream inputStream = toInputStream()) {
            return hash.compute(inputStream);
        }
    }

    @Override
    public String toBase64() {
        return Base64.getEncoder().encodeToString(getBytesReadOnly());
    }

    @Override
    public String toBase64URL() {
        return Base64.getUrlEncoder().encodeToString(getBytesReadOnly());
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            try {
                byte[] hashValue = getHash(Hash.CRC32);
                hashCode = (int)Bytes.toLong(hashValue);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Binary) {
            Binary other = (Binary) obj;
            return Arrays.equals(this.getBytesReadOnly(), other.getBytesReadOnly());
        }
        return false;
    }

    @Override
    public void close() throws IOException {
    }
}
