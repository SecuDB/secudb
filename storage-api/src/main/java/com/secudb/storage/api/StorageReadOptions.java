package com.secudb.storage.api;

import com.secudb.commons.io.IOStreams;
import com.secudb.commons.io.LimitedInputStream;

import java.io.IOException;
import java.io.InputStream;

public class StorageReadOptions {
    public static final StorageReadOptions DEFAULT = StorageReadOptions.builder().build();
    public static final StorageReadOptions IMMUTABLE = StorageReadOptions.builder().initialVersionIfAvailable(true).build();

    private StorageReadOptions() { }

    private boolean fetchInitialVersionIfAvailable;

    private long offset;

    private long length = -1;


    public boolean isFetchInitialVersionIfAvailable() {
        return fetchInitialVersionIfAvailable;
    }

    public boolean hasOffset() {
        return offset > 0;
    }

    public long getOffset() {
        return offset;
    }


    public boolean hasLength() {
        return length != -1;
    }

    public long getLength() {
        return length;
    }


    public static Builder builder() {
        return new Builder();
    }


    public InputStream applyOffset(InputStream inputStream) throws IOException {
        if (hasOffset()) {
            IOStreams.skipExactly(inputStream, getOffset());
        }
        return inputStream;
    }

    public InputStream applyLengthToInputStream(InputStream inputStream) {
        if (hasLength()) {
            return new LimitedInputStream(inputStream, getLength());
        }
        return inputStream;
    }

    public InputStream applyOffsetAndLength(InputStream inputStream) throws IOException {
        return applyLengthToInputStream(applyOffset(inputStream));
    }

    public static class Builder {
        private StorageReadOptions options = new StorageReadOptions();

        public Builder initialVersionIfAvailable() {
            return initialVersionIfAvailable(true);
        }

        public Builder initialVersionIfAvailable(boolean value) {
            options.fetchInitialVersionIfAvailable = value;
            return this;
        }

        public Builder offset(long offset) {
            options.offset = offset;
            return this;
        }

        public Builder length(long length) {
            options.length = length;
            return this;
        }

        public StorageReadOptions build() {
            StorageReadOptions result = options;
            options = null;
            return result;
        }
    }
}
