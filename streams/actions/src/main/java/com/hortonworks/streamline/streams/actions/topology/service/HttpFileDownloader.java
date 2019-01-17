package com.hortonworks.streamline.streams.actions.topology.service;

import com.google.common.base.Strings;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 * Created by santanu.s on 22/09/15.
 */
public class HttpFileDownloader {
    private static final Logger logger = LoggerFactory.getLogger(HttpFileDownloader.class);

    private final String tmpDirectory;
    private final CloseableHttpClient httpClient;


    public HttpFileDownloader(final String namePrefix) throws Exception {
        FileAttribute<Set<PosixFilePermission>> perms = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x"));
        Path createdPath = Files.createTempDirectory(namePrefix, perms);
        this.tmpDirectory = createdPath.toAbsolutePath().toString();

        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(20);

        httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
    }

    public Path download(final String url) {
        HttpGet httpGet = new HttpGet(URI.create(url));
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
            if (HttpStatus.SC_OK != response.getStatusLine().getStatusCode()) {
                throw new RuntimeException(
                        String.format("Server returned [%d][%s] for url: %s",
                                response.getStatusLine().getStatusCode(),
                                response.getStatusLine().getReasonPhrase(),
                                url));
            }
            Header[] headers = response.getHeaders("Content-Disposition");
            String filename = null;
            if (null != headers) {
                for (Header header : headers) {
                    for (HeaderElement headerElement : header.getElements()) {
                        if (!headerElement.getName().equalsIgnoreCase("attachment")) {
                            continue;
                        }
                        NameValuePair attachment = headerElement.getParameterByName("filename");
                        if (attachment != null) {
                            filename = attachment.getValue();
                        }
                    }
                }
            }
            if (Strings.isNullOrEmpty(filename)) {
                String[] nameParts = url.split("/");
                filename = nameParts[nameParts.length - 1];
            }
            return Files.write(Paths.get(this.tmpDirectory, filename), EntityUtils.toByteArray(response.getEntity()));
        } catch (IOException e) {
            throw new RuntimeException("Error loading class from: " + url, e);
        } finally {
            if (null != response) {
                try {
                    response.close();
                } catch (IOException e) {
                    logger.error("Could not close connection to server: ", e);
                }
            }
        }
    }

}
