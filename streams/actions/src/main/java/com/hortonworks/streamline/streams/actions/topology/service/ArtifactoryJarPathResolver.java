package com.hortonworks.streamline.streams.actions.topology.service;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.jfrog.artifactory.client.Artifactory;
import org.jfrog.artifactory.client.ArtifactoryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 * Created by santanu.s on 02/10/15.
 */
public class ArtifactoryJarPathResolver {
  private static final Logger logger = LoggerFactory.getLogger(ArtifactoryJarPathResolver.class);

  private Artifactory client;

  public ArtifactoryJarPathResolver(final String artifactoryUrl) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(artifactoryUrl), "Artifactory URL cannot be null");
    client = ArtifactoryClient.create(artifactoryUrl);

  }

  public String resolve(final String groupId, final String artifactId, final String version) throws Exception {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(groupId), "Group Id cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(artifactId), "Artifact Id cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(version), "Artifact version cannot be null");

    logger.info("Aritifactory client created successfully with uri {}", client.getUri());
    FileAttribute<Set<PosixFilePermission>> perms = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x"));
    java.nio.file.Path tempFilePath = Files.createTempFile(Long.toString(System.currentTimeMillis()), "xml", perms);
    String metadataStr = null;

    metadataStr = String.format("%s/%s/maven-metadata.xml",
        groupId.replaceAll("\\.", "/"),
        artifactId);


    InputStream response = client.repository("maven2")
        .download(metadataStr)
        .doDownload();
    logger.info("download complete");
    Files.copy(response,
        tempFilePath, StandardCopyOption.REPLACE_EXISTING);
    logger.info("Metadata file downloaded to: {}", tempFilePath.toAbsolutePath().toString());

    final String url = String.format("%s%s/%s/%s/%s/%s-%s.jar",
        client.getUri(),
        "maven2",
        groupId.replaceAll("\\.", "/"),
        artifactId,
        version,
        artifactId,
        version);
    logger.info("Jar will be downloaded from: " + url);

    return url;
  }

}
