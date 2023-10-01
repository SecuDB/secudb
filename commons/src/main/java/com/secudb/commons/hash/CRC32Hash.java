package com.secudb.commons.hash;

import java.util.zip.CRC32;

public class CRC32Hash extends AbstractChecksumHash {

    public CRC32Hash() {
        super(CRC32::new);
    }

    @Override
    public String getName() {
        return "CRC32";
    }
}
