package com.secudb.storage.smbcifs;

import com.secudb.storage.api.Storage;
import com.secudb.storage.api.StorageConnector;
import com.secudb.storage.api.credentials.BasicCredentials;

import java.io.IOException;
import java.net.URI;

public class SMB_CIFS_StorageConnector implements StorageConnector {
    @Override
    public String getName() {
        return "Samba/CIFS (org.codelibs.jcifs v2)";
    }

    @Override
    public Storage connect(String name, String connectionString) throws IOException {
        if (!supports(connectionString)) {
            throw new IllegalArgumentException();
        }

        URI uri = URI.create(connectionString);

        String server = uri.getHost();

        int port = uri.getPort();
        if (port != -1) {
            server = server + ":" + port;
        }

        String domain = null;
        String username = null;
        String password = null;

        BasicCredentials credentials = BasicCredentials.fromURI(uri);
        if (credentials != null) {
            username = credentials.getUsername();
            password = credentials.getPassword();
            domain = credentials.getDomain();
        }

        String shareName = null;
        String rootPath = null;

        String path = uri.getPath();
        if (path.length() > 1) {
            int shareNameEnd = path.indexOf('/', 1);
            shareName = path.substring(1, shareNameEnd);

            if (path.length() > shareNameEnd + 1) {
                rootPath = path.substring(shareNameEnd + 1);
            }
        }

        return new SMB_CIFS_Storage(name, server, domain, username, password, shareName, rootPath);
    }

    @Override
    public boolean supports(String connectionString) {
        return connectionString.startsWith("\\\\") || connectionString.startsWith("cifs:/") || connectionString.startsWith("smb:/");
    }

}
