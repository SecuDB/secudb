package com.secudb.commons.binary;

import com.secudb.commons.io.IOStreams;
import com.secudb.commons.io.LimitedInputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class InputStreamBinary extends AbstractBinary {
    private InputStream inputStream;
    private long length;
    private boolean consumed = false;

    public InputStreamBinary(InputStream inputStream) {
        this(inputStream, -1);
    }

    public InputStreamBinary(InputStream inputStream, long length) {
        this.inputStream = inputStream;
        this.length = length;

        if (length == -1 && inputStream instanceof LimitedInputStream) {
            this.length = ((LimitedInputStream) inputStream).getLeft();
        }
    }

    @Override
    public boolean hasLength() {
        return length != -1;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public boolean isSingleReadStream() {
        return true;
    }

    @Override
    public Binary fragment(long offset, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes() {
        ensureNotConsumed();
        consumed = true;

        ByteArrayOutputStream out;
        if (!hasLength()) {
            out = new ByteArrayOutputStream();
        } else {
            out = new ByteArrayOutputStream((int)length);
        }
        writeToUnchecked(out);
        return out.toByteArray();
    }

    @Override
    public long writeTo(OutputStream out) {
        ensureNotConsumed();
        consumed = true;
        return writeToUnchecked(out);
    }

    private long writeToUnchecked(OutputStream out) {
        try {
            long total = 0;
            byte[] buffer = new byte[IOStreams.DEFAULT_BYTE_BUFFER_SIZE];
            int rlen;
            while ((rlen = inputStream.read(buffer)) > 0) {
                out.write(buffer, 0, rlen);
                total += rlen;
            }
            return total;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long writeTo(WritableByteChannel channel) {
        ensureNotConsumed();
        consumed = true;
        try {
            byte[] buffer = new byte[IOStreams.DEFAULT_BYTE_BUFFER_SIZE];
            long total = 0;
            int rlen;
            while ((rlen = inputStream.read(buffer)) > 0) {
                channel.write(ByteBuffer.wrap(buffer, 0, rlen));
                total += rlen;
            }
            return total;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long writeTo(FileChannel fileChannel, long position) {
        ensureNotConsumed();
        consumed = true;
        try {
            byte[] buffer = new byte[IOStreams.DEFAULT_BYTE_BUFFER_SIZE];
            long total = 0;
            int rlen;
            while ((rlen = inputStream.read(buffer)) > 0) {
                fileChannel.write(ByteBuffer.wrap(buffer, 0, rlen), position + total);
                total += rlen;
            }
            return total;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public InputStream toInputStream() {
        return inputStream;
    }

    @Override
    public boolean isInMemory() {
        return false;
    }

    private void ensureNotConsumed() {
        if (consumed) {
            throw new IllegalStateException("Stream already consumed");
        }
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        super.close();
    }
}
