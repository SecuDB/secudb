package com.secudb.commons.hash;

import com.secudb.commons.io.IOStreams;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Supplier;

public abstract class AbstractMessageDigestHash extends AbstractHash {
    private String algorithmName;
    private Supplier<MessageDigest> supplier;

    public AbstractMessageDigestHash(String algorithmName, Supplier<MessageDigest> supplier) {
        this.algorithmName = algorithmName;
        this.supplier = supplier;
    }

    public AbstractMessageDigestHash(String algorithmName) {
        this.algorithmName = algorithmName;
        this.supplier = () -> {
            try {
                return MessageDigest.getInstance(algorithmName);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Hash algorithm not available: " + algorithmName, e);
            }
        };
    }

    @Override
    public String getName() {
        return algorithmName;
    }

    @Override
    public byte[] compute(InputStream inputStream) throws IOException {
        MessageDigest messageDigest = supplier.get();
        byte[] buffer = new byte[IOStreams.DEFAULT_BYTE_BUFFER_SIZE];
        int read;
        while ((read = inputStream.read(buffer, 0, buffer.length)) >= 0) {
            messageDigest.update(buffer, 0, read);
        }
        return messageDigest.digest();
    }

    @Override
    public byte[] compute(byte[] data, int offset, int length) {
        MessageDigest messageDigest = supplier.get();
        messageDigest.update(data, offset, length);
        return messageDigest.digest();
    }
}
