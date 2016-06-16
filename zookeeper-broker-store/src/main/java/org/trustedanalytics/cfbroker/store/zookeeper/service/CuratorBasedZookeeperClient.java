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
package org.trustedanalytics.cfbroker.store.zookeeper.service;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.trustedanalytics.cfbroker.store.helper.PathHelper.normalizePath;
import static org.trustedanalytics.cfbroker.store.zookeeper.service.CuratorExceptionHandler.propagateAsIOException;

public class CuratorBasedZookeeperClient implements ZookeeperClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(CuratorBasedZookeeperClient.class);

    private final CuratorFramework client;
    private final String rootDirectory;

    public CuratorBasedZookeeperClient(CuratorFramework client, String rootDirectory) {
        this.client = client;
        this.rootDirectory = rootDirectory;
    }

    @Override public void init() throws IOException {
        client.start();
    }

    @Override public void destroy() {
        Optional.of(client).ifPresent(CuratorFramework::close);
    }

    @Override public String getRootDir() {
        return rootDirectory;
    }

    @Override public boolean exists(String path) throws IOException {
        String effectivePath = makePath(path);
        return propagateAsIOException(() -> Optional.ofNullable(client.checkExists().forPath(effectivePath)).isPresent(),
            LOGGER::warn, "Error while check znode: " + effectivePath);
    }

    @Override public void addZNode(String path, byte[] zNodeContent) throws IOException {
        String effectivePath = makePath(path);
        propagateAsIOException(
            () -> client.create().creatingParentsIfNeeded().forPath(effectivePath, zNodeContent),
            LOGGER::error, "Error while creating znode: " + effectivePath);
    }

    @Override public byte[] getZNode(String path) throws IOException {
        String effectivePath = makePath(path);
        return propagateAsIOException(() -> client.getData().forPath(effectivePath),
            LOGGER::warn, "Warning while reading znode: " + effectivePath);
    }

    @Override public void deleteZNode(String path) throws IOException {
        String effectivePath = makePath(path);
        propagateAsIOException(
            () -> client.delete().deletingChildrenIfNeeded().forPath(effectivePath),
            LOGGER::error, "Error while deleting znode: " + effectivePath);
    }

    @Override public List<String> getChildrenNames() throws IOException {
        return propagateAsIOException(() -> client.getChildren().forPath(rootDirectory),
                LOGGER::error, "Error while getting children of znode: " + rootDirectory);
    }

    private String makePath(String path) {
        return rootDirectory + normalizePath(path);
    }
}
