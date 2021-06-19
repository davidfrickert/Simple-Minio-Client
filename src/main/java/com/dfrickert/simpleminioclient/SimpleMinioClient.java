package com.dfrickert.simpleminioclient;

import com.dfrickert.simpleminioclient.auth.Credentials;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleMinioClient {
    private static final String SEP = "/";

    private final String minioLocation;
    private final Credentials accessCredentials;
    private HttpClient client;
    private final ExecutorService threadPool;

    public SimpleMinioClient(final String minioLocation,
                             final Credentials accessCredentials,
                             final HttpClient client) {
        this.minioLocation = minioLocation;
        this.accessCredentials = accessCredentials;
        this.client = client;
        this.threadPool = (ExecutorService) client.executor().orElse(null);
    }

    public SimpleMinioClient(final String minioLocation,
                             final Credentials accessCredentials) {
        this(minioLocation, accessCredentials,
                HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_2)
                        .followRedirects(HttpClient.Redirect.NORMAL)
                        .connectTimeout(Duration.ofSeconds(20))
                        .executor(Executors.newCachedThreadPool())
                        .build());
    }

    public InputStream get(final String bucket,
                           final String object) throws IOException, InterruptedException {
        final String bucketUrl = bucket.replace(".", "/");

        final URI uri = URI.create(minioLocation + SEP + bucketUrl + SEP + object);

        final HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(15))
                .headers(headers())
                .GET()
                .build();

        return client.send(request, HttpResponse.BodyHandlers.ofInputStream()).body();
    }

    public void close() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        client = null;
    }

    private String[] headers() {
        return headers(null);
    }

    private String[] headers(byte[] body) {
        List<String> headers = new LinkedList<>();

        String sha256Hash = null;
        String md5Hash = null;
        if (accessCredentials != null) {
            byte[] data = body;

            if (body == null) {
                data = new byte[0];
            }

            sha256Hash = DigestUtils.sha256Hex(data);
            md5Hash = Base64.getEncoder().encodeToString(DigestUtils.md5(data));
        } else if (body != null) {
            md5Hash = Base64.getEncoder().encodeToString(DigestUtils.md5(body));
        }

        if (md5Hash != null) {
            headers.add("Content-MD5");
            headers.add(md5Hash);
        }

        if (sha256Hash != null) {
            headers.add("x-amz-content-sha256");
            headers.add(sha256Hash);
        }

        Instant now = Instant.now();
        DateTimeFormatter AMZ_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                .withZone(ZoneId.from(ZoneOffset.UTC));

        headers.add("x-amz-date");
        headers.add(AMZ_FORMATTER.format(now));

        headers.add("User-Agent");
        headers.add("MinIO (Linux; amd64) minio-java/dev");

        /* Doesn't work - restricted header
        headers.add("Connection");
        headers.add("close");
		/*
		if (length > 0) {
			var requestBody = BodyInserters.fromPublisher(Mono.just(body), byte[].class);
			result = requestBuilder.body(requestBody);
		} else {
			result = requestBuilder;
		}
		 */
        return headers.toArray(new String[0]);
    }
}
