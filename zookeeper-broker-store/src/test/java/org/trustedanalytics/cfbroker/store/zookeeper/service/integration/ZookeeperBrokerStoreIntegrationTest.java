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

import org.trustedanalytics.cfbroker.store.api.BrokerStore;
import org.trustedanalytics.cfbroker.store.api.Location;
import org.trustedanalytics.cfbroker.store.serialization.JSONSerDeFactory;
import org.trustedanalytics.cfbroker.store.zookeeper.service.ZookeeperClient;
import org.trustedanalytics.cfbroker.store.zookeeper.service.ZookeeperClientBuilder;
import org.trustedanalytics.cfbroker.store.zookeeper.service.ZookeeperStore;
import org.trustedanalytics.cfbroker.store.zookeeper.service.utils.ZookeeperTestUtils;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ZookeeperBrokerStoreIntegrationTest {

    private TestingServer zookeeperServer;

    private ZookeeperClient zookeeperClient;

    private BrokerStore<String> store;

    private static String connectionString;
    private static final String ROOT_DIR = "/store/test";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "haselko_maselko";
    private ZookeeperTestUtils.ZookeeperCredentials zookeeperCredentials;

    @Before
    public void setup() throws Exception {
        initTestServer();
        initTestClient();
        store = new ZookeeperStore<>(zookeeperClient,
            JSONSerDeFactory.getInstance().getSerializer(),
            JSONSerDeFactory.getInstance().getDeserializer(String.class));
    }

    private void initTestServer() throws Exception {
        zookeeperServer = new TestingServer();
        zookeeperServer.start();
        connectionString = zookeeperServer.getConnectString();
        zookeeperCredentials =
            new ZookeeperTestUtils.ZookeeperCredentials(connectionString, USERNAME, PASSWORD);
        ZookeeperTestUtils.createDir(zookeeperCredentials, ROOT_DIR);
    }

    private void initTestClient() throws IOException {
        zookeeperClient =
            new ZookeeperClientBuilder(connectionString, USERNAME, PASSWORD, ROOT_DIR).build();
        zookeeperClient.init();
    }

    @After
    public void tearDown() {
        try {
            ZookeeperTestUtils.deleteDir(zookeeperCredentials, ROOT_DIR);
            zookeeperServer.stop();
            zookeeperClient.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void save_saveSimpleObject_objectExistsInZookeeper() throws Exception {
        //arrange

        //act
        byte[] serializedObject = "\"object to save\"".getBytes();
        store.save(Location.newInstance("id"), "object to save");

        //assert
        ZookeeperTestUtils
            .assertZNodeEquals(zookeeperCredentials, ROOT_DIR + "/id", serializedObject);
    }

    @Test
    public void save_saveSimpleObject_objectHasTheSameAclAsParent() throws Exception {
        //act
        store.save(Location.newInstance("id"), "object to save");

        //assert
        ZookeeperTestUtils.assertZNodesAclEquals(zookeeperCredentials, ROOT_DIR, ROOT_DIR + "/id");
    }

    @Test
    public void saveWithPath_saveSimpleObject_objectExistsInZookeeper() throws Exception {
        //arrange

        //act
        byte[] serializedObject = "\"object to save\"".getBytes();
        store.save(Location.newInstance("id", "path"), "object to save");

        //assert
        ZookeeperTestUtils.assertZNodeEquals(zookeeperCredentials, ROOT_DIR + "/path/id",
            serializedObject);
    }

    @Test
    public void saveWithPath_saveSimpleObject_objectHasTheSameAclAsParent() throws Exception {
        //act
        store.save(Location.newInstance("id", "path"), "object to save");

        //assert
        ZookeeperTestUtils
            .assertZNodesAclEquals(zookeeperCredentials, ROOT_DIR, ROOT_DIR + "/path");
        ZookeeperTestUtils
            .assertZNodesAclEquals(zookeeperCredentials, ROOT_DIR, ROOT_DIR + "/path/id");
    }

    @Test
    public void getById_existingPath_returnsObject() throws Exception {
        //arrange
        byte[] serializedObject = "\"object to save\"".getBytes();
        ZookeeperTestUtils
            .saveBytesInZNode(zookeeperCredentials, ROOT_DIR + "/id", serializedObject);

        //act
        Optional<String> actualObject = store.getById(Location.newInstance("id"));

        //assert
        assertThat(actualObject.get(), equalTo("object to save"));
    }

    @Test
    public void getById_notExistingPath_returnsEmptyOptional() throws Exception {
        //arrange

        //act
        Optional<String> actualObject = store.getById(Location.newInstance("id"));

        //assert
        assertThat(actualObject, equalTo(Optional.empty()));
    }

    @Test
    public void getByIdWithPath_existingPath_returnsObject() throws Exception {
        //arrange
        byte[] serializedObject = "\"object to save\"".getBytes();
        ZookeeperTestUtils
            .saveBytesInZNode(zookeeperCredentials, ROOT_DIR + "/path/id", serializedObject);

        //act
        Optional<String> actualObject = store.getById(Location.newInstance("id", "path"));

        //assert
        assertThat(actualObject.get(), equalTo("object to save"));
    }

    @Test
    public void getByIdWithPath_notExistingPath_returnsEmptyOptional() throws Exception {
        //arrange

        //act
        Optional<String> actualObject = store.getById(Location.newInstance("id", "path"));

        //assert
        assertThat(actualObject, equalTo(Optional.empty()));
    }

    @Test
    public void deleteById_existingPath_deletesAndReturnsOBject() throws Exception {
        //arrange
        byte[] serializedObject = "\"object to save\"".getBytes();
        ZookeeperTestUtils
            .saveBytesInZNode(zookeeperCredentials, ROOT_DIR + "/id", serializedObject);

        //act
        Optional<String> returnedObject = store.deleteById(Location.newInstance("id"));

        //assert
        ZookeeperTestUtils.assertZNodeNotExist(zookeeperCredentials, ROOT_DIR + "/id");
        assertThat(returnedObject.get(), equalTo("object to save"));
    }

    @Test
    public void deleteById_existingZNodeWithChild_deletesAndReturnsOBject() throws Exception {
        //arrange
        byte[] serializedObject = "\"object to save\"".getBytes();
        ZookeeperTestUtils
            .saveBytesInZNode(zookeeperCredentials, ROOT_DIR + "/id", serializedObject);
        byte[] serializedChild = "\"child to save\"".getBytes();
        ZookeeperTestUtils
            .saveBytesInZNode(zookeeperCredentials, ROOT_DIR + "/id/childId", serializedChild);

        //act
        Optional<String> returnedObject = store.deleteById(Location.newInstance("id"));

        //assert
        ZookeeperTestUtils.assertZNodeNotExist(zookeeperCredentials, ROOT_DIR + "/id");
        assertThat(returnedObject.get(), equalTo("object to save"));
    }

    @Test
    public void deleteById_notExistingPath_returnsEmptyOptional() throws Exception {
        //arrange

        //act
        Optional<String> returnedObject = store.deleteById(Location.newInstance("id"));

        //assert
        assertThat(returnedObject, equalTo(Optional.empty()));
    }

    @Test
    public void deleteByIdWithPath_existingPath_deletesAndReturnsOBject() throws Exception {
        //arrange
        byte[] serializedObject = "\"object to save\"".getBytes();
        ZookeeperTestUtils
            .saveBytesInZNode(zookeeperCredentials, ROOT_DIR + "/path/id", serializedObject);

        //act
        Optional<String> returnedObject = store.deleteById(Location.newInstance("id", "path"));

        //assert
        ZookeeperTestUtils.assertZNodeNotExist(zookeeperCredentials, ROOT_DIR + "/id");
        assertThat(returnedObject.get(), equalTo("object to save"));
    }

    @Test
    public void deleteByIdWithPath_notExistingPath_returnsEmptyOptional() throws Exception {
        //arrange

        //act
        Optional<String> returnedObject = store.deleteById(Location.newInstance("id", "path"));

        //assert
        assertThat(returnedObject, equalTo(Optional.empty()));
    }
}
