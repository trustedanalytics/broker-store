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

import com.google.common.base.Preconditions;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class ZookeeperClientBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperClientBuilder.class);

    private final String zookeeperConnectionString;
    private final String username;
    private final String password;
    private final String rootDirectory;
    private RetryPolicy retryPolicy;

    public ZookeeperClientBuilder(String zookeeperConnectionString, String username,
        String password, String rootDirectory) throws IOException {

        this.zookeeperConnectionString = zookeeperConnectionString;
        this.username = username;
        this.password = password;
        this.rootDirectory = getNormalizedRootDir(rootDirectory);
        this.retryPolicy = new ExponentialBackoffRetry(1000, 3);
    }

    private String getNormalizedRootDir(String dir) {
        Preconditions.checkArgument(dir.startsWith("/"), "Root dir must starts with \"/\"");
        Preconditions.checkArgument(!dir.endsWith("/"), "Root dir can't start with \"/\"");
        return dir;
    }

    public ZookeeperClientBuilder withRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    public ZookeeperClient build() throws IOException {
        CuratorFramework client = buildCuratorClient(new ConstACLProvider(getRootACLs()));
        return new CuratorBasedZookeeperClient(client, rootDirectory);
    }

    private List<ACL> getRootACLs() throws IOException {

        CuratorFramework client = buildCuratorClient();
        client.start();

        CuratorExceptionHandler.propagateAsIOException(
                () -> checkIfRootDirectoryExists(client),
                LOGGER::error, "Root directory: " + rootDirectory + " doesn't exists");

        List<ACL> aclList = CuratorExceptionHandler.propagateAsIOException(
                () -> client.getACL().forPath(rootDirectory),
                LOGGER::error, "Error getting ACL for : " + rootDirectory);

        client.close();

        return aclList;
    }

    private Void checkIfRootDirectoryExists(CuratorFramework client) throws Exception {
        Preconditions.checkArgument(client.checkExists().forPath(rootDirectory) != null,
            "Check exists: " + rootDirectory + " returns null");
        return null;
    }

    private CuratorFramework buildCuratorClient() {
        return curatorClientTemplate()
            .build();
    }

    private CuratorFramework buildCuratorClient(ACLProvider aclProvider) {
        return curatorClientTemplate()
            .aclProvider(aclProvider)
            .build();
    }

    private CuratorFrameworkFactory.Builder curatorClientTemplate() {
        return CuratorFrameworkFactory.builder()
            .connectString(zookeeperConnectionString)
            .retryPolicy(retryPolicy)
            .authorization("digest",
                String.format("%s:%s", username, password).getBytes(Charset.defaultCharset()));
    }
}
