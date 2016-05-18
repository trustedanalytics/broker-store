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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface HdfsClient {

    /**
     * Root directory without "/" at the end.
     */
    String getRootDir();

    void createDir(String path) throws IOException;

    void setOwner(String path, String owner, String group) throws IOException;

    void setPermission(String path, FsPermission fsPermission) throws IOException;

    /**
     * Creates key and encrypted zone using that key.
     * @param keyName - name of the encryption key
     * @param path - path denoting directory that will be encrypted
     * @throws IOException
    */
    void createKeyAndEncryptedZone(String keyName, Path path) throws IOException;

    void createEmptyFile(String path) throws IOException;

    Optional<byte[]> getPathAttr(String path, String name) throws IOException;

    void addPathAttr(String path, String name, byte[] value) throws IOException;

    List<byte[]> getDirectSubPathsAttrs(String path, String attrName) throws IOException;

    void deleteById(String path) throws IOException;

}
