package com.dfrickert.simpleminioclient;

import com.dfrickert.simpleminioclient.auth.Credentials;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.Header;
import org.apache.http.client.methods.*;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;

public class SimpleMinioClient {
    private static final String SEP = "/";

    private final String minioLocation;
    private final Credentials accessCredentials;
    private CloseableHttpClient client;

    public SimpleMinioClient(final String minioLocation,
                             final Credentials accessCredentials,
                             final CloseableHttpClient client) {
        this.minioLocation = minioLocation;
        this.accessCredentials = accessCredentials;
        this.client = client;
    }

    public SimpleMinioClient(final String minioLocation,
                             final Credentials accessCredentials) {
        this(minioLocation, accessCredentials,
                HttpClientBuilder.create().build());
    }

    public InputStream get(final String bucket,
                           final String object) throws IOException, InterruptedException {
        final String bucketUrl = bucket.replace(".", "/");

        final URI uri = URI.create(minioLocation + SEP + bucketUrl + SEP + object);

        final HttpRequestBase request = new HttpGet(uri);
        request.setHeaders(headers());

        final CloseableHttpResponse response = client.execute(request);

        final InputStream content = response.getEntity().getContent();

        return content;
    }

    public void put(final String bucket,
                    final  String fileName,
                    final InputStream object) throws IOException {
        final String bucketUrl = bucket.replace(".", "/");

        final URI uri = URI.create(minioLocation + SEP + bucketUrl + SEP + fileName);

        final HttpEntityEnclosingRequestBase request = new HttpPut(uri);
        byte[] bytes = object.readAllBytes();
        request.setHeaders(headers(bytes));
        request.setEntity(new ByteArrayEntity(bytes));

        client.execute(request);
    }

    public void close() {
        try {
            client.close();
        } catch (IOException ignored) {

        }
        client = null;
    }

    private Header[] headers() {
        return headers(null);
    }

    private Header[] headers(byte[] body) {
        List<Header> headers = new LinkedList<>();

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
            headers.add(new BasicHeader("Content-MD5", md5Hash));
        }

        if (sha256Hash != null) {
            headers.add(new BasicHeader("x-amz-content-sha256", sha256Hash));
        }

        Instant now = Instant.now();
        DateTimeFormatter AMZ_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                .withZone(ZoneId.from(ZoneOffset.UTC));

        headers.add(new BasicHeader("x-amz-date", AMZ_FORMATTER.format(now)));

        headers.add(new BasicHeader("User-Agent", "MinIO (Linux; amd64) minio-java/dev"));

        headers.add(new BasicHeader("Connection", "close"));

        /*
		if (length > 0) {
			var requestBody = BodyInserters.fromPublisher(Mono.just(body), byte[].class);
			result = requestBuilder.body(requestBody);
		} else {
			result = requestBuilder;
		}

         */

        return headers.toArray(new Header[0]);
    }
}
