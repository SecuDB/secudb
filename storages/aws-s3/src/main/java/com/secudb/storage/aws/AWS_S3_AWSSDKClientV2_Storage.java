package com.secudb.storage.aws;

import com.secudb.commons.binary.Binary;
import com.secudb.commons.hash.Hash;
import com.secudb.commons.io.LimitedInputStream;
import com.secudb.storage.api.AbstractSyncStorage;
import com.secudb.storage.api.StorageReadOptions;
import com.secudb.storage.api.StorageWriteOptions;
import com.secudb.storage.api.exceptions.AlreadyExistsInStorageException;
import com.secudb.storage.api.exceptions.NotFoundInStorageException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.List;

public class AWS_S3_AWSSDKClientV2_Storage extends AbstractSyncStorage {
    private String name;
    private S3Client client;
    private String bucket;
    private String rootPath;


    public AWS_S3_AWSSDKClientV2_Storage(String name, S3Client client, String bucket) {
        this(name, client, bucket, null);
    }

    public AWS_S3_AWSSDKClientV2_Storage(String name, S3Client client, String bucket, String rootPath) {
        this.name = name;
        this.client = client;
        this.bucket = bucket;
        this.rootPath = normalizeRootPath(rootPath);
    }

    private static String normalizeRootPath(String rootPath) {
        if (rootPath == null || rootPath.isEmpty()) {
            return "";
        }
        return rootPath + "/";
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean exists(String path) throws IOException {
        try {
            getObjectHead(path);
            return true;
        } catch (NoSuchKeyException noSuchKeyException) {
            return false;
        } catch (S3Exception e) {
            throw translateException(e);
        }
    }

    private HeadObjectResponse getObjectHead(String path) {
        return client.headObject(
                HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(resolveObjectKey(path))
                        .build()
        );
    }

    @Override
    public long length(String path) throws IOException {
        try {
            HeadObjectResponse head = getObjectHead(path);
            return head.contentLength();
        } catch (NoSuchKeyException noSuchKeyException) {
            throw new NotFoundInStorageException();
        } catch (S3Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public byte[] readFully(String path, StorageReadOptions options) throws IOException {
        if (!options.hasOffset() && !options.hasLength()) {
            ResponseBytes<GetObjectResponse> responseBytes = client.getObjectAsBytes(prepareGetObjectRequest(path, options));
            return responseBytes.asByteArrayUnsafe();
        }
        return super.readFully(path, options);
    }

    @Override
    public InputStream readStream(String path, StorageReadOptions options) throws IOException {
        try {
            ResponseInputStream<GetObjectResponse> responseInputStream = client.getObject(prepareGetObjectRequest(path, options));
            GetObjectResponse response = responseInputStream.response();
            InputStream inputStream = options.applyOffsetAndLength(responseInputStream);
            if (!options.hasLength() && response != null && response.contentLength() != null) {
                return new LimitedInputStream(inputStream, response.contentLength() - options.getOffset());
            }
            return inputStream;
        } catch (NoSuchKeyException noSuchKeyException) {
            throw new NotFoundInStorageException(noSuchKeyException);
        }
    }

    @Override
    public void write(String path, Binary data, StorageWriteOptions options) throws IOException {
        if (data.isSingleReadStream()) {
            throw new IllegalArgumentException("Data stream not supported");
        }

        try {
            PutObjectRequest.Builder requestBuilder = preparePutObjectRequestBuilder(path, options);

            String checksumSHA256 = Base64.getEncoder().encodeToString(data.getHash(Hash.SHA2_256));
            requestBuilder.checksumSHA256(checksumSHA256);

            if (!options.isAllowOverwrite() && exists(path)) {
                throw new AlreadyExistsInStorageException();
            }

            RequestBody requestBody;
            if (data.isInMemory()) {
                requestBody = RequestBody.fromBytes(data.getBytesReadOnly());
            } else {
                requestBody = RequestBody.fromInputStream(data.toInputStream(), data.getLength());
            }
            PutObjectResponse resp = client.putObject(requestBuilder.build(), requestBody);

            if (!options.isAllowOverwrite()) {
                ensurePutVersionIsFirst(path, resp.versionId());
            }
        } catch (S3Exception e) {
            throw translateException(e);
        }
    }

    @Override
    public void delete(String path) throws NotFoundInStorageException, IOException {
        try {
            ObjectVersion firstVersion = getFirstVersion(path);

            client.deleteObjects(
                    DeleteObjectsRequest.builder()
                            .bucket(bucket)
                            .delete(Delete.builder().objects(
                                    ObjectIdentifier.builder().key(path).versionId(firstVersion.versionId()).build(),
                                    ObjectIdentifier.builder().key(path).build()
                            ).build())
                            .bypassGovernanceRetention(true)
                            .build()
            );
        } catch (S3Exception e) {
            throw translateException(e);
        }
    }

    protected ObjectVersion getFirstVersion(String path) throws NotFoundInStorageException {
        ListObjectVersionsResponse response = client.listObjectVersions(
                ListObjectVersionsRequest.builder()
                        .bucket(bucket)
                        .prefix(resolveObjectKey(path))
                        .build()
        );

        ObjectVersion firstVersion = null;
        List<ObjectVersion> versions = response.versions();
        for (ObjectVersion ov : versions) {
            if (ov.key().equals(path)) {
                if (firstVersion == null || ov.lastModified().compareTo(firstVersion.lastModified()) <= 0) {
                    firstVersion = ov;
                }
            }
        }

        if (firstVersion == null) {
            throw new NotFoundInStorageException();
        }

        return firstVersion;
    }

    protected ObjectVersion getLatestVersion(String path) {
        HeadObjectResponse head = getObjectHead(path);

        return ObjectVersion.builder()
                .key(resolveObjectKey(path))
                .versionId(head.versionId())
                .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                .checksumAlgorithmWithStrings(head.checksumSHA256())
                .isLatest(true)
                .eTag(head.eTag())
                .size(head.contentLength())
                .lastModified(head.lastModified())
                .build();
    }

    protected GetObjectRequest prepareGetObjectRequest(String path, StorageReadOptions options) throws NotFoundInStorageException, IOException {
        GetObjectRequest.Builder builder = GetObjectRequest.builder()
                .bucket(bucket)
                .key(resolveObjectKey(path))
                ;

        if (options.isFetchInitialVersionIfAvailable()) {
            ObjectVersion firstVersion = getFirstVersion(path);
            String versionId = firstVersion.versionId();
            builder.versionId(versionId);
        }

        return builder.build();
    }

    protected PutObjectRequest.Builder preparePutObjectRequestBuilder(String path, StorageWriteOptions options) {
        return PutObjectRequest.builder()
                .bucket(bucket)
                .key(resolveObjectKey(path))
                ;
    }

    protected void ensurePutVersionIsFirst(String path, String versionId) throws NotFoundInStorageException, AlreadyExistsInStorageException {
        ObjectVersion firstVersion = getFirstVersion(path);
        if (!versionId.equals(firstVersion.versionId())) {
            try {
                client.deleteObject(
                        DeleteObjectRequest.builder().bucket(bucket).key(resolveObjectKey(path)).versionId(versionId).build()
                );
            } catch (S3Exception ignore) {
                //ignoring because it might be locked, it's fine
            }

            throw new AlreadyExistsInStorageException();
        }
    }

    protected String resolveObjectKey(String path) {
        return rootPath + path;
    }

    protected IOException translateException(S3Exception e) {
        if (e instanceof NoSuchKeyException) {
            return new NotFoundInStorageException(e);
        }
        return new IOException(e);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
