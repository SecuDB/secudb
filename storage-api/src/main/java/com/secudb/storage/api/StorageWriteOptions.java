package com.secudb.storage.api;

import java.time.Instant;

public class StorageWriteOptions {
    public static final StorageWriteOptions OVERWRITE = StorageWriteOptions.builder().allowOverwrite(true).build();
    public static final StorageWriteOptions IMMUTABLE = StorageWriteOptions.builder().allowOverwrite(false).build();

    private StorageWriteOptions() { }

    private boolean allowOverwrite;
    private Instant expirationDate;

    public boolean isAllowOverwrite() {
        return allowOverwrite;
    }

    public Instant getExpirationDate() {
        return expirationDate;
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private StorageWriteOptions options = new StorageWriteOptions();

        public Builder allowOverwrite() {
            return allowOverwrite(true);
        }

        public Builder allowOverwrite(boolean value) {
            options.allowOverwrite = value;
            return this;
        }

        public Builder expirationDate(Instant value) {
            options.expirationDate = value;
            return this;
        }

        public StorageWriteOptions build() {
            StorageWriteOptions result = options;
            options = null;
            return result;
        }
    }
}
