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
package org.trustedanalytics.cfbroker.store.zookeeper.service.integration;

import java.util.Arrays;
import java.util.List;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.trustedanalytics.cfbroker.store.zookeeper.service.ZookeeperClient;
import org.trustedanalytics.cfbroker.store.zookeeper.service.ZookeeperClientBuilder;
import org.trustedanalytics.cfbroker.store.zookeeper.service.utils.ZookeeperTestUtils;

public class ZookeeperClientBuilderTest {

  private static final String ROOT_DIR = "/store/test";
  private static final String USERNAME = "admin";
  private static final String PASSWORD = "haselko_maselko";
  private static String connectionString;
  private TestingServer zookeeperServer;
  private ZookeeperTestUtils.ZookeeperCredentials zookeeperCredentials;

  @Before
  public void setup() throws Exception {
    initTestServer();

  }

  @After
  public void tearDown() {
    try {
      zookeeperServer.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void build_createWithoutRootCreation_rootExists() throws Exception {
    ZookeeperTestUtils.createDir(zookeeperCredentials, ROOT_DIR);
    testZookeeperClientWithoutRootCreation();
  }

  @Test(expected = IllegalArgumentException.class)
  public void build_createWithoutRootCreation_rootNotExists() throws Exception {
    testZookeeperClientWithoutRootCreation();
  }

  @Test
  public void build_createWithRootCreation_rootExists() throws Exception {
    ZookeeperTestUtils.createDir(zookeeperCredentials, ROOT_DIR);
    testZookeeperClientWithRootCreation();
  }

  @Test
  public void build_createWithRootCreation_rootNotExists() throws Exception {
    testZookeeperClientWithRootCreation();
  }

  private void testZookeeperClientWithoutRootCreation() throws Exception {
    ZookeeperClient zkClient =
        new ZookeeperClientBuilder(connectionString, USERNAME, PASSWORD, ROOT_DIR).build();
    zkClient.init();
    zkClient.exists(ROOT_DIR);
    zkClient.destroy();
  }

  private void testZookeeperClientWithRootCreation() throws Exception {
    String digest =
        DigestAuthenticationProvider.generateDigest(String.format("%s:%s", USERNAME, PASSWORD));
    List<ACL> acl = Arrays.asList(new ACL(ZooDefs.Perms.ALL, new Id("digest", digest)));

    ZookeeperClient zkClient =
        new ZookeeperClientBuilder(connectionString, USERNAME, PASSWORD, ROOT_DIR)
            .withRootCreation(acl).build();
    zkClient.init();
    zkClient.exists(ROOT_DIR);
    zkClient.destroy();
  }

  private void initTestServer() throws Exception {
    zookeeperServer = new TestingServer();
    zookeeperServer.start();
    connectionString = zookeeperServer.getConnectString();
    zookeeperCredentials =
        new ZookeeperTestUtils.ZookeeperCredentials(connectionString, USERNAME, PASSWORD);
  }

}
