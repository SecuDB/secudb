package com.secudb.storage.all;

import com.secudb.storage.api.StorageConnectors;

public class Main {
    public static void main(String[] args) {
        new StorageConnectors().forEach(storageConnector -> System.out.println(
                storageConnector.getName() + " : " + storageConnector.getClass().getName()
        ));
    }
}
