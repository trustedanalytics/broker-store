/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.cfbroker.store.hdfs.service;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.cfbroker.store.hdfs.helper.DirHelper;

import com.google.common.base.Preconditions;

public class SimpleHdfsClient implements HdfsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleHdfsClient.class);
    private static final String DIRECTORY_NOT_EXISTS = "Directory doesn't exists : ";

    private final FileSystem fs;

    public SimpleHdfsClient(FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public void addPathAttr(String path, String name, byte[] value) throws IOException {
        fs.setXAttr(getNormalizedPath(path), name, value);
    }

    @Override
    public Optional<byte[]> getPathAttr(String path, String name) throws IOException {
        return getPathAttr(getNormalizedPath(path), name);
    }

    private Optional<byte[]> getPathAttr(Path path, String name) throws IOException {
        LOGGER.info("Checking attributes on path " + path);
        if (fs.exists(path))
            return Optional.ofNullable(fs.getXAttrs(path).get(name));
        return Optional.empty();
    }

    @Override
    public List<byte[]> getDirectSubPathsAttrs(String path, String attrName)
            throws IOException {

        List<byte[]> attrs = new ArrayList<>();
        Path p = getNormalizedPath(path);
        if (!fs.isDirectory(p))
            throw new IllegalArgumentException("Path : " + path + ", should be a directory");

        FileStatus[] listStatus = fs.listStatus(p);
        for (FileStatus status : listStatus) {
            Optional<byte[]> pathAttr = getPathAttr(status.getPath(), attrName);
            if (pathAttr.isPresent())
                attrs.add(pathAttr.get());
        }
        return attrs;
    }

    @Override
    public String getRootDir() {
        return "/";
    }

    @Override
    public void createDir(String path) throws IOException {
        Optional<Path> p = getPathIfNotExsits(path);
        if(p.isPresent())
          fs.mkdirs(p.get());
    }

    @Override
    public void createDir(String path, FsPermission permission) throws IOException {
      Optional<Path> p = getPathIfNotExsits(path);
      if(p.isPresent())
        fs.mkdirs(p.get(), permission);
    }

    @Override
    public void addAclEntry(String path, AclEntry aclEntry) throws IOException {
        Path p = new Path(path);
        LOGGER.info("Add Acl to directory {} {}", path, aclEntry);

        if(!fs.exists(p)) {
          throw new IOException(DIRECTORY_NOT_EXISTS + path);
        }
        fs.modifyAclEntries(p, Arrays.asList(aclEntry));
    }

    @Override
    public void setOwner(String path, String owner, String group) throws IOException {
        Path p = getNormalizedPath(path);
        LOGGER.info("Set directory {} owner to: {}", path, owner);

        if(!fs.exists(p)) {
            throw new IOException(DIRECTORY_NOT_EXISTS + path);
        }

        fs.setOwner(p, owner, group);
        String directoryOwner = fs.getFileStatus(p).getOwner();
        if (!directoryOwner.equals(owner))
            throw new IOException(String.format("The dir owner hasn't ben changed to: %s - current owner: %s",
                owner, directoryOwner));
    }

    @Override
    public void setPermission(String path, FsPermission fsPermission) throws IOException {
        Path p = getNormalizedPath(path);
        LOGGER.info("Changing directory permissions: " + p);

        if(!fs.exists(p)) {
            throw new IOException(DIRECTORY_NOT_EXISTS + path);
        }

        fs.setPermission(p, fsPermission);
    }

    @Override
    public void createKeyAndEncryptedZone(String keyName, Path path) throws IOException {
        if(fs.exists(path)){
            try {
                createEncryptionZoneKey(keyName);
            } catch (NoSuchAlgorithmException e) {
                fs.delete(path, true);
                throw new IOException("Error while creating encryption dir: " + path, e);
            }

            DistributedFileSystem dfs = (DistributedFileSystem)fs;
            dfs.createEncryptionZone(path, keyName);
        } else {
            throw new IOException(DIRECTORY_NOT_EXISTS + path);
        }
    }

    @Override
    public void createEmptyFile(String path) throws IOException {
        Path p = getNormalizedPath(path);
        if (!fs.createNewFile(p) && !fs.exists(p))
            throw new IOException("Error while creating file : " + p);
    }

    @Override
    public void deleteById(String path) throws IOException {
        Path p = getNormalizedPath(path);
        if (!fs.delete(p, true))
            throw new IOException("Error while deleting path : " + p);
    }

    void createEncryptionZoneKey(String key) throws NoSuchAlgorithmException, IOException {
        final KeyProvider.Options options = KeyProvider.options(fs.getConf());
        options.setDescription(key);
        options.setBitLength(256);

        List<KeyProvider> providers = KeyProviderFactory.getProviders(fs.getConf());
        Preconditions.checkArgument(!providers.isEmpty(), "KMS configuration required for creating encryption zones");

        KeyProvider keyProvider = providers.get(0);
        keyProvider.createKey(key, options);
    }

    private Path getNormalizedPath(String dir) {
        return new Path(DirHelper.addLeadingSlash(DirHelper.removeLeadingSlashes(dir)));
    }

    private Optional<Path> getPathIfNotExsits(String path) throws IOException {
      Path p = getNormalizedPath(path);
      LOGGER.info("Creating directory: " + p);

      if(fs.exists(p)) {
        LOGGER.info("Path already exists, nothing to create: " + p);
        return Optional.empty();
      }
      return Optional.of(p);
    }
}
