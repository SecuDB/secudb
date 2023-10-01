package com.secudb.commons.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LimitedInputStream extends FilterInputStream {
    private long limit;
    private long left;

    public LimitedInputStream(InputStream in, long offset, long limit) throws IOException {
        this(in, limit);
        if (offset > 0) {
            IOStreams.skipExactly(in, offset);
        }
    }

    public LimitedInputStream(InputStream in, long limit) {
        super(in);
        this.limit = limit;
        this.left = limit;
    }

    public long getLimit() {
        return limit;
    }

    public long getLeft() {
        return left;
    }

    @Override
    public int available() throws IOException {
        if (left > 0) {
            int available = super.available();
            if (available > left) {
                return (int)left;
            }
            return available;
        }
        return 0;
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public int read() throws IOException {
        if (left > 0) {
            return super.read();
        }
        return -1;
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (left > 0) {
            int r;
            if (b.length > left) {
                r = super.read(b, 0, (int)left);
            } else {
                r = super.read(b);
            }
            left -= r;
            return r;
        }
        return -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (left > 0) {
            int r;
            if (len > left) {
                r = super.read(b, off, (int)left);
            } else {
                r = super.read(b, off, len);
            }
            left -= r;
            return r;
        }
        return -1;
    }

    @Override
    public long skip(long n) throws IOException {
        if (left > 0) {
            long skipped;
            if (n > left) {
                skipped = super.skip(left);
            } else {
                skipped = super.skip(n);
            }
            left -= skipped;
            return skipped;
        }
        return 0;
    }
}
