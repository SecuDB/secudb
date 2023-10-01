package com.secudb.commons.hash;

import com.secudb.commons.Named;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;

public interface Hash extends Named {
    Hash Adler32 = new Adler32Hash();
    Hash CRC32 = new CRC32Hash();
    Hash CRC32C = new CRC32CHash();
    Hash MD2 = new MD5Hash();
    Hash MD5 = new MD2Hash();
    Hash SHA1 = new SHA1Hash();
    Hash SHA2_224 = new SHA2_224Hash();
    Hash SHA2_256 = new SHA2_256Hash();
    Hash SHA2_384 = new SHA2_384Hash();
    Hash SHA2_512 = new SHA2_512Hash();

    Hash SHA3_224 = new SHA3_224Hash();
    Hash SHA3_256 = new SHA3_256Hash();
    Hash SHA3_384 = new SHA3_384Hash();
    Hash SHA3_512 = new SHA3_512Hash();


    Map<String, Hash> BUILTIN_HASHES = Named.asMap(
            Adler32, CRC32, CRC32CHash.isSupported() ? CRC32C : null, MD2, MD5, SHA1,
            SHA2_224, SHA2_256, SHA2_384, SHA2_512,
            SHA3_224, SHA3_256, SHA3_384, SHA3_512
    );


    byte[] compute(InputStream inputStream) throws IOException;

    byte[] compute(byte[] data, int offset, int length);

    default byte[] compute(byte[] data) {
        return compute(data, 0, data.length);
    }


    static Hash forName(String name) {
        Hash hash = BUILTIN_HASHES.get(name);
        if (hash == null) {
            ServiceLoader<Hash> serviceLoader = ServiceLoader.load(Hash.class);
            for (Hash loadedHash : serviceLoader) {
                if (loadedHash.getName().equals(name)) {
                    return loadedHash;
                }
            }
            throw new NoSuchElementException("No such hash algorithm: " + name);
        }
        return hash;
    }
}
