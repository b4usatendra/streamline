package com.hortonworks.streamline.streams.actions.topology.service;

import com.google.common.base.*;
import org.apache.http.*;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.*;
import org.apache.http.impl.conn.*;
import org.apache.http.util.*;
import org.slf4j.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;

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
