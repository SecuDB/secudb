package com.secudb.commons.hash;

import java.lang.reflect.InvocationTargetException;
import java.util.zip.Checksum;

/**
 * Requires Java 9+
 */
public class CRC32CHash extends AbstractChecksumHash {
    private final static String JAVA_CLASS = "java.util.zip.CRC32C";

    public CRC32CHash() {
        super(() -> {
            try {
                return (Checksum) Class.forName(JAVA_CLASS).getDeclaredConstructor().newInstance();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("CRC32C supported from Java 9+", e);
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException |
                     NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public String getName() {
        return "CRC32C";
    }


    public static boolean isSupported() {
        try {
            Class.forName(JAVA_CLASS);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
