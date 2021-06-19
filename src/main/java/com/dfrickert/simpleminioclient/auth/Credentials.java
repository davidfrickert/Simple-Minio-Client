package com.dfrickert.simpleminioclient.auth;

public class Credentials {
    private final String accessKey;
    private final String secretKey;

    public Credentials(String accessKey, String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }
}