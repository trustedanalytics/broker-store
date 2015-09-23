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

import com.google.common.base.Preconditions;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.cfbroker.store.hdfs.helper.DirHelper;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ChrootedHdfsClient implements HdfsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChrootedHdfsClient.class);

    private final FileSystem fs;

    private final String rootDir;

    public ChrootedHdfsClient(FileSystem fs, String rootDir)
            throws IOException {
        this.fs = fs;
        this.rootDir = getNormalizedRootDir(rootDir);
        initialize(fs, rootDir);
    }

    private void initialize(FileSystem fs, String rootDir) throws IOException {
        Preconditions.checkState(fs.exists(new Path(rootDir)), "Root directory : " + rootDir + " doesn't exists");
    }

    @Override
    public void addPathAttr(String path, String name, byte[] value) throws IOException {
        fs.setXAttr(getChrootedPath(path), name, value);
    }

    @Override
    public Optional<byte[]> getPathAttr(String path, String name) throws IOException {
        return getChrootedPathAttr(getChrootedPath(path), name);
    }

    private Optional<byte[]> getChrootedPathAttr(Path path, String name) throws IOException {
        LOGGER.info("Checking attributes on path " + path);
        if (fs.exists(path))
            return Optional.ofNullable(fs.getXAttrs(path).get(name));
        return Optional.empty();
    }

    @Override
    public List<byte[]> getDirectSubPathsAttrs(String path, String attrName)
            throws IOException {

        List<byte[]> attrs = new ArrayList<>();
        Path chrootedPath = getChrootedPath(path);
        if (!fs.isDirectory(chrootedPath))
            throw new IllegalArgumentException("Path : " + path + ", should be a directory");

        FileStatus[] listStatus = fs.listStatus(chrootedPath);
        for (FileStatus status : listStatus) {
            Optional<byte[]> pathAttr = getChrootedPathAttr(status.getPath(), attrName);
            if (pathAttr.isPresent())
                attrs.add(pathAttr.get());
        }
        return attrs;
    }

    @Override
    public void createDir(String relativePath) throws IOException {
        Path path = getChrootedPath(relativePath);

        if (!fs.mkdirs(path) && !fs.exists(path))
            throw new IOException("Error while creating dirs : " + path);
    }

    @Override
    public void createEncryptedDir(String relativePath) throws IOException {
        Path path = getChrootedPath(relativePath);
        fs.mkdirs(path);
        try {
            createEncryptionZoneKey(relativePath);
        } catch (NoSuchAlgorithmException e) {
            fs.delete(path, false);
            throw new IOException("Error while creating encryption dir: " + relativePath, e);
        }

        HdfsAdmin admin = new HdfsAdmin(fs.getUri(), fs.getConf());
        admin.createEncryptionZone(path, relativePath);
    }

    @Override
    public void createEmptyFile(String relativePath) throws IOException {
        Path path = getChrootedPath(relativePath);

        if (!fs.createNewFile(path) && !fs.exists(path))
            throw new IOException("Error while creating file : " + path);
    }

    @Override
    public void deleteById(String relativePath) throws IOException {
        Path path = getChrootedPath(relativePath);
        if (!fs.delete(path, true))
            throw new IOException("Error while deleting path : " + path);
    }

    void createEncryptionZoneKey(String key) throws NoSuchAlgorithmException, IOException {
        final KeyProvider.Options options = KeyProvider.options(fs.getConf());
        options.setDescription(key);
        options.setBitLength(128);

        KeyProvider keyProvider = KeyProviderFactory.getProviders(fs.getConf()).get(0);
        keyProvider.createKey(key, options);
    }

    String getNormalizedRootDir(String dir) {
        Preconditions.checkArgument(dir.startsWith("/"), "Root dir must starts with \"/\"");
        return DirHelper.removeTrailingSlashes(dir);
    }

    Path getChrootedPath(String relativePath) {
        if (relativePath.startsWith("/"))
            return new Path(rootDir + relativePath);
        return new Path(rootDir + "/" + relativePath);
    }

    @Override
    public String getRootDir() {
        return rootDir;
    }

}
