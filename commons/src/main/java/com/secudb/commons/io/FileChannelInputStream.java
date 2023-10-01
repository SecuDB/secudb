package com.secudb.commons.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelInputStream extends InputStream {
    private FileChannel fileChannel;

    private ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);
    private boolean closeFileChannelOnStreamClose;

    private long position;

    public FileChannelInputStream(FileChannel fileChannel, long position, boolean closeFileChannelOnStreamClose) {
        this.fileChannel = fileChannel;
        this.position = position;
        this.closeFileChannelOnStreamClose = closeFileChannelOnStreamClose;
    }

    private int read(ByteBuffer byteBuffer) throws IOException {
        int count = fileChannel.read(byteBuffer, position);
        position += count;
        return count;
    }

    @Override
    public int read() throws IOException {
        singleByteBuffer.rewind();
        int count = fileChannel.read(singleByteBuffer, position);
        position += count;
        if (count == 0) {
            return -1;
        }
        return singleByteBuffer.get(0);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(ByteBuffer.wrap(b));
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return read(ByteBuffer.wrap(b, off, len));
    }

    @Override
    public void close() throws IOException {
        if (closeFileChannelOnStreamClose) {
            fileChannel.close();
        }
    }
}
