package com.secudb.commons.binary;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;

public class CompositeBinary extends AbstractBinary {
    private Collection<Binary> components;

    public CompositeBinary() {
        components = new CopyOnWriteArrayList<>();
    }

    public CompositeBinary(Collection<Binary> components) {
        this.components = new CopyOnWriteArrayList<>(components);
    }

    public void add(Binary component) {
        this.components.add(component);
    }

    @Override
    public boolean hasLength() {
        for (Binary binary : components) {
            if (!binary.hasLength()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long getLength() {
        long length = 0;
        for (Binary component : components) {
            length += component.getLength();
        }
        return length;
    }

    @Override
    public boolean isSingleReadStream() {
        for (Binary binary : components) {
            if (binary.isSingleReadStream()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public byte[] getBytes() {
        int offset = 0;

        if (hasLength()) {
            byte[] target = new byte[(int) getLength()];
            for (Binary component : components) {
                byte[] source = component.getBytesReadOnly();
                System.arraycopy(source, 0, target, offset, source.length);
                offset += source.length;
            }
            return target;
        }
        else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            for (Binary component : components) {
                component.writeTo(baos);
            }
            return baos.toByteArray();
        }
    }

    @Override
    public long writeTo(OutputStream out) {
        long length = 0;
        for (Binary component : components) {
            length += component.writeTo(out);
        }
        return length;
    }

    @Override
    public long writeTo(WritableByteChannel channel) {
        long length = 0;
        for (Binary component : components) {
            length += component.writeTo(channel);
        }
        return length;
    }

    @Override
    public long writeTo(FileChannel fileChannel, long position) {
        long length = 0;
        for (Binary component : components) {
            length += component.writeTo(fileChannel, position + length);
        }
        return length;
    }

    @Override
    public Binary fragment(long offset, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isInMemory() {
        for (Binary binary : components) {
            if (!binary.isInMemory()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Binary toMemory() {
        if (!isInMemory()) {
            CompositeBinary newComposite = new CompositeBinary();
            for (Binary oldComponent : components) {
                newComposite.add(oldComponent.toMemory());
            }
            return newComposite;
        }
        return this;
    }

    @Override
    public void close() {
        for (Binary component : components) {
            try {
                component.close();
            } catch (Exception e) {
                e.printStackTrace(); //TODO: logging
            }
        }
        components = Collections.emptyList();
    }
}
