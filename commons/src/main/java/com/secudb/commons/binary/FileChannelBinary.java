package com.secudb.commons.binary;

import com.secudb.commons.io.FileChannelInputStream;
import com.secudb.commons.io.IOStreams;
import com.secudb.commons.io.LimitedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelBinary extends AbstractBinary {

    protected FileChannel fileChannel;
    protected long position;
    protected long length;

    public FileChannelBinary(FileChannel fileChannel) {
        try {
            this.fileChannel = fileChannel;
            this.position = fileChannel.position();
            this.length = fileChannel.size() - this.position;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public FileChannelBinary(FileChannel fileChannel, long position, long length) {
        this.fileChannel = fileChannel;
        this.position = position;
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
        byte[] target = new byte[(int)length];
        int buflen = (int)Math.min(IOStreams.DEFAULT_BYTE_BUFFER_SIZE, length);
        ByteBuffer buf = ByteBuffer.allocate(buflen);
        int offset = 0;

        try {
            while (offset < length) {
                int r = fileChannel.read(buf, position + offset);
                if (r == -1) {
                    break;
                }

                if (r > 0) {
                    if (buf.hasArray()) {
                        System.arraycopy(buf.array(), buf.arrayOffset(), target, offset, r);
                    } else {
                        buf.rewind();
                        buf.get(target, offset, r);
                    }

                    offset += r;

                    buf.rewind();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return target;
    }

    @Override
    public InputStream toInputStream() {
        return new LimitedInputStream(new FileChannelInputStream(fileChannel, position, false), length);
    }

    @Override
    public boolean isInMemory() {
        return false;
    }

    @Override
    public Binary fragment(long offset, long length) {
        return new FileChannelBinary(fileChannel, this.position + offset, length);
    }
}
