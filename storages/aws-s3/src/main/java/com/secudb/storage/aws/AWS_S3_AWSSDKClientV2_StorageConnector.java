package com.secudb.storage.aws;

import com.secudb.storage.api.Storage;
import com.secudb.storage.api.StorageConnector;
import com.secudb.storage.api.credentials.BasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;
import java.net.URISyntaxException;

public class AWS_S3_AWSSDKClientV2_StorageConnector implements StorageConnector {
    @Override
    public String getName() {
        return "AWS S3 (AWS SDK for Java v2)";
    }

    @Override
    public Storage connect(String name, String connectionString) {
        if (!supports(connectionString)) {
            throw new IllegalArgumentException();
        }

        S3ClientBuilder builder = S3Client.builder();

        String bucket = "SecuDB";
        String rootPath = null;

        if (!connectionString.equals("s3")) {
            URI uri = URI.create(connectionString);

            String host = uri.getHost();
            String region;
            if (host.startsWith("s3-")) {
                region = host.substring(3);
            } else {
                region = host;
            }

            if (region.endsWith(".amazonaws.com")) {
                region = region.substring(0, region.length() - ".amazonaws.com".length());
            }
            builder.region(Region.of(region));

            String path = uri.getPath();
            if (path.length() > 1) {
                int bucketEnd = path.indexOf('/', 1);
                bucket = path.substring(1, bucketEnd);

                if (path.length() > bucketEnd + 1) {
                    rootPath = path.substring(bucketEnd + 1);
                }
            }

            BasicCredentials credentials = BasicCredentials.fromURI(uri);
            if (credentials != null) {
                AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(
                        credentials.getUsername(),
                        credentials.getPassword());
                builder.credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials));
            }

            if (name == null || name.isEmpty()) {
                try {
                    name = new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment()).toString();
                } catch (URISyntaxException ignore) {
                    name = "s3";
                }
            }
        }

        if (name == null || name.isEmpty()) {
            name = "s3";
        }

        return new AWS_S3_AWSSDKClientV2_Storage(name, builder.build(), bucket, rootPath);
    }

    @Override
    public boolean supports(String connectionString) {
        return connectionString.equals("s3") || connectionString.startsWith("s3://") || (connectionString.startsWith("https://s3-") && connectionString.contains(".amazonaws.com"));
    }
}
