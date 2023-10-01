package com.secudb.storage.smbcifs;

import com.secudb.commons.binary.Binary;
import com.secudb.commons.io.IOStreams;
import com.secudb.storage.api.AbstractSyncStorage;
import com.secudb.storage.api.StorageReadOptions;
import com.secudb.storage.api.StorageWriteOptions;
import com.secudb.storage.api.exceptions.AlreadyExistsInStorageException;
import com.secudb.storage.api.exceptions.NotFoundInStorageException;
import jcifs.CIFSContext;
import jcifs.CIFSException;
import jcifs.SmbResource;
import jcifs.context.SingletonContext;
import jcifs.smb.NtlmPasswordAuthenticator;
import jcifs.smb.SmbException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

public class SMB_CIFS_Storage extends AbstractSyncStorage {
    private String name;
    private CIFSContext context;
    private String url;

    public SMB_CIFS_Storage(String name, String server, String username, String password, String shareName, String rootPath) {
        this(name, server, new NtlmPasswordAuthenticator(username, password), shareName, rootPath);
    }

    public SMB_CIFS_Storage(String name, String server, String domain, String username, String password, String shareName, String rootPath) {
        this(name, server, new NtlmPasswordAuthenticator(domain, username, password), shareName, rootPath);
    }

    public SMB_CIFS_Storage(String name, String server, NtlmPasswordAuthenticator authenticator, String shareName, String rootPath) {
        this(name, server, SingletonContext.getInstance().withCredentials(authenticator), shareName, rootPath);
    }

    public SMB_CIFS_Storage(String name, String server, CIFSContext context, String shareName, String rootPath) {
        this.name = name;
        this.context = context;
        url = "smb://" + server + "/" + shareName;
        if (rootPath == null || !rootPath.startsWith("/")) {
            url += "/";
        }
        if (rootPath != null && !rootPath.isEmpty()) {
            url += rootPath;
        }
        if (!url.endsWith("/")) {
            url += '/';
        }
    }

    @Override
    public String getName() {
        return name;
    }

    protected String resolve(String path) {
        return url + path;
    }

    public SMB_CIFS_Storage(String url) {
        this.context = SingletonContext.getInstance();
        this.url = url;
    }

    @Override
    public boolean exists(String path) throws IOException {
        try {
            return context.get(resolve(path)).exists();
        } catch (CIFSException e) {
            throw translateException(e);
        }
    }

    @Override
    public long length(String path) throws IOException {
        return context.get(resolve(path)).length();
    }

    @Override
    public InputStream readStream(String path, StorageReadOptions options) throws IOException {
        try {
            InputStream inputStream = context.get(resolve(path)).openInputStream();
            return options.applyOffsetAndLength(inputStream);
        } catch (CIFSException e) {
            throw translateException(e);
        }
    }

    @Override
    public void write(String path, Binary data, StorageWriteOptions options) throws IOException {
        try {
            SmbResource targetResource = context.get(resolve(path));
            if (!options.isAllowOverwrite() && targetResource.exists()) {
                throw new AlreadyExistsInStorageException();
            }

            SmbResource parentResource = targetResource.resolve(".");
            if (!parentResource.exists()) {
                parentResource.mkdirs();
            }

            SmbResource tempResource = context.get(resolve(path + "." + UUID.randomUUID() + ".tmp"));
            tempResource.createNewFile();
            try {
                try (InputStream in = data.toInputStream()) {
                    try (OutputStream out = tempResource.openOutputStream()) {
                        IOStreams.transfer(in, out);
                    }
                }

                tempResource.renameTo(targetResource, options.isAllowOverwrite());
            } finally {
                if (tempResource.exists()) {
                    tempResource.delete();
                }
            }
        } catch (CIFSException e) {
            throw translateException(e);
        }
    }

    @Override
    public void delete(String path) throws NotFoundInStorageException, IOException {
        try {
            context.get(resolve(path)).delete();
        } catch (CIFSException e) {
            throw translateException(e);
        }
    }

    protected IOException translateException(CIFSException e) {
        if (e instanceof SmbException) {
            long status = ((SmbException) e).getNtStatus();
            if (status == -1073741772) {
                return new NotFoundInStorageException(e);
            }
            if (status == -1073741771) {
                return new AlreadyExistsInStorageException(e);
            }
        }
        return e;
    }

    @Override
    public void close() throws Exception {
        if (context != null) {
            context.close();
            context = null;
        }
    }
}
