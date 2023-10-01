package com.secudb.storage.memory;

import org.junit.jupiter.api.Test;

import java.io.IOException;

class MemoryStorageTest {
    @Test
    void test() throws IOException {
        new MemoryStorage("test").testReadWrite();
    }
}
