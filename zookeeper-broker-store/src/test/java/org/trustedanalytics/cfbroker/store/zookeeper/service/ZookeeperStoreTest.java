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

import org.trustedanalytics.cfbroker.store.api.BrokerStore;
import org.trustedanalytics.cfbroker.store.api.Location;
import org.trustedanalytics.cfbroker.store.serialization.RepositoryDeserializer;
import org.trustedanalytics.cfbroker.store.serialization.RepositorySerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ZookeeperStoreTest {

    @Mock
    private CuratorBasedZookeeperClient zookeeper;

    @Mock
    private RepositorySerializer<String> serializer;

    @Mock
    private RepositoryDeserializer<String> deserializer;

    private BrokerStore<String> store;

    private static final Location SIMPLE_LOCATION = Location.newInstance("id");
    private static final Location COMPLEX_LOCATION = Location.newInstance("id", "parentID");
    private static final String SIMPLE_PATH = "/id";
    private static final String COMPLEX_PATH = "/parentID/id";
    private static final String TEST_OBJECT = "whatever";

    @Before
    public void setup() throws IOException {
        store = new ZookeeperStore<>(zookeeper, serializer, deserializer);

        when(serializer.serialize(TEST_OBJECT))
            .thenReturn(TEST_OBJECT.getBytes());

        when(deserializer.deserialize(TEST_OBJECT.getBytes()))
            .thenReturn(new String(TEST_OBJECT.getBytes()));
    }

    @Test
    public void save_success_savesSerializedObjectAsZNode() throws Exception {
        store.save(SIMPLE_LOCATION, TEST_OBJECT);
        verify(zookeeper).addZNode(SIMPLE_PATH, TEST_OBJECT.getBytes());
    }

    @Test
    public void saveComplexPath_success_savesSerializedObjectAsZNode() throws Exception {
        store.save(COMPLEX_LOCATION, TEST_OBJECT);
        verify(zookeeper).addZNode(COMPLEX_PATH, TEST_OBJECT.getBytes());
    }

    @Test
    public void getById_ZNodePathExists_returnsDeserializedObject() throws Exception {
        when(zookeeper.getZNode(SIMPLE_PATH)).thenReturn(TEST_OBJECT.getBytes());
        assertThat(store.getById(SIMPLE_LOCATION).get(), equalTo(TEST_OBJECT));
    }

    @Test
    public void getById_zookeeperThrowsException_returnsEmptyOptional() throws Exception {
        when(zookeeper.getZNode("nonexistent-id")).thenThrow(new IOException());
        Optional<String> actual = store.getById(Location.newInstance("nonexistent-id"));
        assertThat(actual, equalTo(Optional.empty()));
    }

    @Test
    public void getByIdComplexPath_ZNodePathExists_returnsDeserializedObject() throws Exception {
        when(zookeeper.getZNode(COMPLEX_PATH)).thenReturn(TEST_OBJECT.getBytes());
        assertThat(store.getById(COMPLEX_LOCATION).get(), equalTo(TEST_OBJECT));
    }

    @Test
    public void getByIdComplexPath_zookeeperThrowsException_returnsEmptyOptional() throws Exception {
        when(zookeeper.getZNode("/path/nonexistent-id")).thenThrow(new IOException());
        Optional<String> actual = store.getById(Location.newInstance("nonexistent-id", "path"));
        assertThat(actual, equalTo(Optional.empty()));
    }

    @Test
    public void delete_ZNodePathExists_deletesAndReturnsDeserializedObject() throws IOException {
        when(zookeeper.getZNode(SIMPLE_PATH)).thenReturn(TEST_OBJECT.getBytes());
        Optional<String> actual = store.deleteById(SIMPLE_LOCATION);
        assertThat(actual.get(), equalTo(TEST_OBJECT));
        verify(zookeeper).deleteZNode(SIMPLE_PATH);
    }

    @Test
    public void delete_zookeeperReturnsNull_returnsEmptyOptional() throws IOException {
        when(zookeeper.getZNode(COMPLEX_PATH)).thenReturn(null);
        Optional<String> actual = store.deleteById(COMPLEX_LOCATION);
        assertThat(actual, equalTo(Optional.empty()));
    }

    @Test
    public void delete_zookeeperThrowsException_returnsEmptyOptional() throws IOException {
        when(zookeeper.getZNode("/path/nonexistent-id")).thenThrow(new IOException());
        Optional<String> actual = store.deleteById(Location.newInstance("nonexistent-id", "path"));
        assertThat(actual, equalTo(Optional.empty()));
    }

    @Test
    public void deleteComplexPath_ZNodePathExists_deletesAndReturnsDeserializedObject()
        throws IOException {

        when(zookeeper.getZNode(COMPLEX_PATH)).thenReturn(TEST_OBJECT.getBytes());
        Optional<String> actual = store.deleteById(COMPLEX_LOCATION);
        assertThat(actual.get(), equalTo(TEST_OBJECT));
        verify(zookeeper).deleteZNode(COMPLEX_PATH);
    }

    @Test
    public void deleteComplexPath_zookeeperReturnsNull_returnsEmptyOptional() throws IOException {
        when(zookeeper.getZNode(COMPLEX_PATH)).thenReturn(null);
        Optional<String> actual = store.deleteById(COMPLEX_LOCATION);
        assertThat(actual, equalTo(Optional.empty()));
    }

    @Test
    public void deleteComplexPath_zookeeperThrowsException_returnsEmptyOptional() throws IOException {
        when(zookeeper.getZNode("/path/nonexistent-id")).thenThrow(new IOException());
        Optional<String> actual = store.deleteById(Location.newInstance("nonexistent-id", "path"));
        assertThat(actual, equalTo(Optional.empty()));
    }
}
