package com.secudb.commons;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public interface Named {
    String getName();


    @SafeVarargs
    static <T extends Named> Map<String, T> asMap(T... array) {
        Map<String, T> map = new LinkedHashMap<>();
        for (T element : array) {
            if (element != null) {
                map.put(element.getName(), element);
            }
        }
        return Collections.unmodifiableMap(map);
    }

    static <T extends Named> Map<String, T> asMap(Collection<T> collection) {
        Map<String, T> map = new LinkedHashMap<>();
        for (T element : collection) {
            if (element != null) {
                map.put(element.getName(), element);
            }
        }
        return Collections.unmodifiableMap(map);
    }
}
