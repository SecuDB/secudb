package com.secudb.commons.hash;

import java.util.zip.Adler32;

public class Adler32Hash extends AbstractChecksumHash {

    public Adler32Hash() {
        super(Adler32::new);
    }

    @Override
    public String getName() {
        return "Adler-32";
    }
}
