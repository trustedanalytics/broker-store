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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class XAttrsHdfsStoreTest {

    private static final String ATTR = "user.String";

    @Mock
    private ChrootedHdfsClient hdfs;

    @Mock
    private RepositorySerializer<String> serializer;

    @Mock
    private RepositoryDeserializer<String> deserializer;

    private BrokerStore<String> store;

    @Before
    public void setup() throws IOException {
        store = new XAttrsHdfsStore<>(hdfs, serializer, deserializer, ATTR);
    }

    @Test
    public void testSave_success_savesSerializedObjectAsAttributeOnHdfsPath() throws Exception {
        String testedObject = "junit";
        when(serializer.serialize(testedObject)).thenReturn(testedObject.getBytes());
        store.save(Location.newInstance("id"), testedObject);
        verify(hdfs).createDir("/id");
        verify(hdfs).addPathAttr("/id", ATTR, testedObject.getBytes());
    }

    @Test
    public void testSaveWithParent_success_savesSerializedObjectAsAttributeOnHdfsPath() throws Exception {
        String testedObject = "junit";
        when(serializer.serialize(testedObject)).thenReturn(testedObject.getBytes());
        store.save(Location.newInstance("id", "path"), testedObject);
        verify(hdfs).createDir("/path/id");
        verify(hdfs).addPathAttr("/path/id", ATTR, testedObject.getBytes());
    }

    @Test
    public void testGetById_HdfsPathWithAttrExists_returnsDeserializedObject() throws Exception {
        String testedObject = "junit";
        when(hdfs.getPathAttr("/id", ATTR)).thenReturn(Optional.of(testedObject.getBytes()));
        when(deserializer.deserialize(testedObject.getBytes())).thenReturn(
            new String(testedObject.getBytes()));
        assertThat(store.getById(Location.newInstance("id")).get(), equalTo(testedObject));
    }

    @Test
    public void testGetByIdWithParent_HdfsPathWithAttrExists_returnsDeserializedObject() throws Exception {
        String testedObject = "junit";
        when(hdfs.getPathAttr("/path/id", ATTR)).thenReturn(Optional.of(testedObject.getBytes()));
        when(deserializer.deserialize(testedObject.getBytes())).thenReturn(
            new String(testedObject.getBytes()));
        assertThat(store.getById(Location.newInstance("id", "path")).get(),
            equalTo(testedObject));
    }

    @Test
    public void testGetById_HdfsPathWithoutAttrExists_returnsOptionalEmpty() throws Exception {
        when(hdfs.getPathAttr("/id", ATTR)).thenReturn(Optional.empty());
        assertFalse(store.getById(Location.newInstance("id")).isPresent());
    }

    @Test(expected = IOException.class)
    public void testGetById_hdfsThrowsException_rethrowsException() throws Exception {
        when(hdfs.getPathAttr("/nonexistent", ATTR)).thenThrow(new IOException());
        store.getById(Location.newInstance("nonexistent"));
    }

    @Test
    public void testDeleteById_HdfsPathWithAttrExists_returnsDeserializedObject() throws Exception {
        String testedObject = "junit";
        when(hdfs.getPathAttr("/id", ATTR)).thenReturn(Optional.of(testedObject.getBytes()));
        when(deserializer.deserialize(testedObject.getBytes())).thenReturn(
            new String(testedObject.getBytes()));
        assertThat(store.deleteById(Location.newInstance("id")).get(), equalTo(testedObject));
        verify(hdfs).deleteById("/id");
    }

    @Test
    public void testDeleteByIdWithParent_HdfsPathWithAttrExists_returnsDeserializedObject() throws Exception {
        String testedObject = "junit";
        when(hdfs.getPathAttr("/path/id", ATTR)).thenReturn(Optional.of(testedObject.getBytes()));
        when(deserializer.deserialize(testedObject.getBytes())).thenReturn(
            new String(testedObject.getBytes()));
        assertThat(store.deleteById(Location.newInstance("id", "path")).get(),
            equalTo(testedObject));
        verify(hdfs).deleteById("/path/id");
    }

    @Test
    public void testDeleteById_HdfsPathWithoutAttrExists_returnsOptionalEmpty() throws Exception {
        when(hdfs.getPathAttr("/id", ATTR)).thenReturn(Optional.empty());
        assertFalse(store.deleteById(Location.newInstance("id")).isPresent());
    }

    @Test(expected = IOException.class)
    public void testDeleteById_hdfsThrowsException_rethrowsException() throws Exception {
        when(hdfs.getPathAttr("/nonexistent", ATTR)).thenThrow(new IOException());
        store.deleteById(Location.newInstance("/nonexistent"));
    }
}
