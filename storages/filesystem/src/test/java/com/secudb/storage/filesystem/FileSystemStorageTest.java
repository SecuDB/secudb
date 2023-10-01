package com.secudb.storage.filesystem;

import com.secudb.commons.io.FileSystemUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class FileSystemStorageTest {
    @Test
    void test() throws IOException {
        Path path = Files.createTempDirectory("filesystem-storage-test");
        try {
            new FileSystemStorage("test", path).testReadWrite();
        } finally {
            FileSystemUtils.deleteDirectory(path);
        }
    }
}
