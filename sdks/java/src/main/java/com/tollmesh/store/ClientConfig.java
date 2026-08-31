package com.tollmesh.store;

/**
 * Configuration for TollMeshCache client
 */
public class ClientConfig {
    private String host = "localhost";
    private int port = 8080;
    private double timeout = 5.0;
    private boolean verifySSL = true;
    private String apiKey;
    private String scheme = "http";
    private int maxRetries = 3;
    private double retryBackoff = 1.0;
    private int connectionPoolSize = 10;

    /**
     * Create configuration with defaults
     */
    public ClientConfig() {}

    /**
     * Get base URL
     */
    public String getBaseUrl() {
        return String.format("%s://%s:%d", scheme, host, port);
    }

    // Getters and setters
    public String getHost() { return host; }
    public ClientConfig setHost(String host) { this.host = host; return this; }

    public int getPort() { return port; }
    public ClientConfig setPort(int port) { this.port = port; return this; }

    public double getTimeout() { return timeout; }
    public ClientConfig setTimeout(double timeout) { this.timeout = timeout; return this; }

    public boolean isVerifySSL() { return verifySSL; }
    public ClientConfig setVerifySSL(boolean verifySSL) { this.verifySSL = verifySSL; return this; }

    public String getApiKey() { return apiKey; }
    public ClientConfig setApiKey(String apiKey) { this.apiKey = apiKey; return this; }

    public String getScheme() { return scheme; }
    public ClientConfig setScheme(String scheme) { this.scheme = scheme; return this; }

    public int getMaxRetries() { return maxRetries; }
    public ClientConfig setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; return this; }

    public double getRetryBackoff() { return retryBackoff; }
    public ClientConfig setRetryBackoff(double retryBackoff) { this.retryBackoff = retryBackoff; return this; }

    public int getConnectionPoolSize() { return connectionPoolSize; }
    public ClientConfig setConnectionPoolSize(int size) { this.connectionPoolSize = size; return this; }

    @Override
    public String toString() {
        return "ClientConfig{" +
               "host='" + host + '\'' +
               ", port=" + port +
               ", timeout=" + timeout +
               ", verifySSL=" + verifySSL +
               ", scheme='" + scheme + '\'' +
               '}';
    }
}
