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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CuratorBasedZookeeperClientTest {

    private static final String ROOT_DIRECTORY = "/cf/test/dir";
    private static final String TEST_PATH = "inner_path/id";
    private static final byte[] TEST_OBJECT = "object".getBytes();

    private CuratorFramework curatorClient;
    private ZookeeperClient client;

    @Before
    public void setup() throws IOException {
        curatorClient = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
        client = new CuratorBasedZookeeperClient(curatorClient, ROOT_DIRECTORY);
    }

    @Test
    public void addZNode_addingSuccessful_methodShouldComplete() throws Exception {
        when(curatorClient.create().creatingParentsIfNeeded()
            .forPath(ROOT_DIRECTORY + "/" + TEST_PATH, TEST_OBJECT))
            .thenReturn("whatever");

        client.addZNode(TEST_PATH, TEST_OBJECT);

        verify(curatorClient, atLeastOnce()).create();
    }

    @Test(expected = IOException.class)
    public void addZNode_addingFails_shouldPropagateAsIOException() throws Exception {
        when(curatorClient.create().creatingParentsIfNeeded()
            .forPath(ROOT_DIRECTORY + "/" + TEST_PATH, TEST_OBJECT))
            .thenThrow(new Exception());

        client.addZNode(TEST_PATH, TEST_OBJECT);
    }

    @Test
    public void getZNode_getSuccessful_shouldReturnZNodeContent() throws Exception {
        when(curatorClient.getData().forPath(ROOT_DIRECTORY + "/" + TEST_PATH))
            .thenReturn(TEST_OBJECT);

        byte[] actualObject = client.getZNode(TEST_PATH);

        assertThat(actualObject, equalTo(TEST_OBJECT));
        verify(curatorClient, atLeastOnce()).getData();
    }

    @Test(expected = IOException.class)
    public void getZNode_getFails_shouldPropagateAsIOException() throws Exception {
        when(curatorClient.getData().forPath(ROOT_DIRECTORY + "/" + TEST_PATH))
            .thenThrow(new Exception());

        client.getZNode(TEST_PATH);
    }

    @Test
    public void deleteZNode_deleteSuccessful_shouldDeleteZNode() throws Exception {
        when(curatorClient.delete().deletingChildrenIfNeeded()
            .forPath(ROOT_DIRECTORY + "/" + TEST_PATH))
            .thenReturn(null);

        client.deleteZNode(TEST_PATH);

        verify(curatorClient, atLeastOnce()).delete();
    }

    @Test(expected = IOException.class)
    public void deleteZNode_deleteFails_shouldPropagateAsIOException() throws Exception {
        when(curatorClient.delete().deletingChildrenIfNeeded()
            .forPath(ROOT_DIRECTORY + "/" + TEST_PATH))
            .thenThrow(new Exception());

        client.deleteZNode(TEST_PATH);
    }

    @Test
    public void getChildrenNames_listingSuccessful_shouldListChildren() throws Exception {
        when(curatorClient.getChildren().forPath(ROOT_DIRECTORY))
            .thenReturn(null);

        client.getChildrenNames();

        verify(curatorClient, atLeastOnce()).getChildren();
    }

    @Test(expected = IOException.class)
    public void getChildrenNames_listingFails_shouldPropagateAsIOException() throws Exception {
        when(curatorClient.getChildren().forPath(ROOT_DIRECTORY))
            .thenThrow(new Exception());

        client.getChildrenNames();
    }

    @Test
    public void checkIfExists_notExists_returnFalse() throws Exception {
        String path = ROOT_DIRECTORY + "/" + TEST_PATH;
        when(curatorClient.checkExists().forPath(path)).thenReturn(null);

        boolean result = client.exists(TEST_PATH);
        assertThat(result, equalTo(false));
    }

    @Test
    public void checkIfExists_exists_returnTrue() throws Exception {
      String path = ROOT_DIRECTORY + "/" + TEST_PATH;
      Stat testObjectStat = new Stat();
      when(curatorClient.checkExists().forPath(path)).thenReturn(testObjectStat);

      boolean result = client.exists(TEST_PATH);
      assertThat(result, equalTo(true));
    }

    @Test(expected = IOException.class)
    public void checkIfExists_throwException_shouldPropagateAsIOException() throws Exception {
      String path = ROOT_DIRECTORY + "/" + TEST_PATH;
      when(curatorClient.checkExists().forPath(path)).thenThrow(new Exception());

      boolean result = client.exists(TEST_PATH);
    }

}
