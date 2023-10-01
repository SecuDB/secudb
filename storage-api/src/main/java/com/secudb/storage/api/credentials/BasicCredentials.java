package com.secudb.storage.api.credentials;

import java.net.URI;

public class BasicCredentials implements Credentials {
    private String username;
    private String password;
    private String domain;

    public BasicCredentials(String domain, String username, String password) {
        this(username, password);
        this.domain = domain;
    }

    public BasicCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getDomain() {
        return domain;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public String toString() {
        if (domain != null && !domain.isEmpty()) {
            return username + "@" + domain;
        }
        return username;
    }

    public static BasicCredentials fromURI(URI uri) {
        String domain = null;
        String username;
        String password = null;

        String userInfo = uri.getUserInfo();
        if (userInfo != null && !userInfo.isEmpty()) {
            int domainSplitter = userInfo.indexOf(';');
            int passwordSplitter = userInfo.indexOf(':');

            if (domainSplitter != -1 && (domainSplitter < passwordSplitter || passwordSplitter == -1)) {
                domain = userInfo.substring(0, domainSplitter);
                userInfo = userInfo.substring(domainSplitter + 1);
                passwordSplitter = userInfo.indexOf(':');
            }

            if (passwordSplitter == -1) {
                username = userInfo;
            }
            else {
                username = userInfo.substring(0, passwordSplitter);
                password = userInfo.substring(passwordSplitter + 1);
            }

            return new BasicCredentials(domain, username, password);
        }

        return null;
    }
}
