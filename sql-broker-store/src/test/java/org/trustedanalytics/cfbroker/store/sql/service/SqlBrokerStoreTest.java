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
package org.trustedanalytics.cfbroker.store.sql.service;

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
import java.sql.SQLException;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class SqlBrokerStoreTest {

  @Mock
  private BrokerSqlClient brokerSqlClient;

  @Mock
  private RepositorySerializer<String> serializer;

  @Mock
  private RepositoryDeserializer<String> deserializer;

  private BrokerStore<String> store;

  private Location serviceInstance;

  private Location bindingInstance;

  @Before
  public void setup() throws IOException {
    serviceInstance = Location.newInstance("serviceInstanceId");
    bindingInstance = Location.newInstance("serviceId", "bindingInstanceId");
    store = new SqlBrokerStore<>(brokerSqlClient, serializer, deserializer);
  }

  @Test
  public void testSave_success_savesSerializedObjectInServiceTable() throws Exception {
    String testedObject = "serviceInstance";
    when(serializer.serialize(testedObject)).thenReturn(testedObject.getBytes());
    store.save(serviceInstance, testedObject);
    verify(brokerSqlClient).insertInstance(serviceInstance.getId(), testedObject.getBytes());
  }

  @Test
  public void testSaveWithParent_success_savesSerializedObjectInBindingTable() throws Exception {
    String testedObject = "bindingInstance";
    when(serializer.serialize(testedObject)).thenReturn(testedObject.getBytes());
    store.save(bindingInstance, testedObject);
    verify(brokerSqlClient).insertBinding(bindingInstance.getId(),
        bindingInstance.getParentId().get(), testedObject.getBytes());
  }

  @Test
  public void testGetById_ServiceWitchObjectExists_returnsDeserializedObject() throws Exception {
    String testedObject = "serviceInstance";
    when(brokerSqlClient.selectInstance(serviceInstance.getId()))
        .thenReturn(Optional.of(testedObject.getBytes()));
    when(deserializer.deserialize(testedObject.getBytes()))
        .thenReturn(new String(testedObject.getBytes()));
    assertThat(store.getById(serviceInstance).get(), equalTo(testedObject));
  }

  @Test
  public void testGetByIdWithParent_BindingWitchObjectExists_returnsDeserializedObject()
      throws Exception {
    String testedObject = "bindingInstance";
    when(
        brokerSqlClient.selectBinding(bindingInstance.getId(), bindingInstance.getParentId().get()))
            .thenReturn(Optional.of(testedObject.getBytes()));
    when(deserializer.deserialize(testedObject.getBytes()))
        .thenReturn(new String(testedObject.getBytes()));
    assertThat(store.getById(bindingInstance).get(), equalTo(testedObject));
  }

  @Test
  public void testGetById_ServiceWithoutObjectExists_returnsOptionalEmpty() throws Exception {
    when(brokerSqlClient.selectInstance(serviceInstance.getId())).thenReturn(Optional.empty());
    assertFalse(store.getById(serviceInstance).isPresent());
  }

  @Test
  public void testGetById_SqlBrokerThrowsException_returnOptionalEmpty() throws Exception {
    when(brokerSqlClient.selectInstance(serviceInstance.getId())).thenThrow(new SQLException());
    store.getById(serviceInstance);
  }

  @Test
  public void testDeleteById_ServiceWithObjectExists_returnsDeserializedObject() throws Exception {
    String testedObject = "serviceInstance";
    when(brokerSqlClient.selectInstance(serviceInstance.getId()))
        .thenReturn(Optional.of(testedObject.getBytes()));
    when(deserializer.deserialize(testedObject.getBytes()))
        .thenReturn(new String(testedObject.getBytes()));
    assertThat(store.deleteById(serviceInstance).get(), equalTo(testedObject));
    verify(brokerSqlClient).deleteInstance(serviceInstance.getId());
  }

  @Test
  public void testDeleteByIdWithParent_BindingWithObjectExists_returnsDeserializedObject()
      throws Exception {
    String testedObject = "bindingInstance";
    when(
        brokerSqlClient.selectBinding(bindingInstance.getId(), bindingInstance.getParentId().get()))
            .thenReturn(Optional.of(testedObject.getBytes()));
    when(deserializer.deserialize(testedObject.getBytes()))
        .thenReturn(new String(testedObject.getBytes()));
    assertThat(store.deleteById(bindingInstance).get(), equalTo(testedObject));
    verify(brokerSqlClient).deleteBinding(bindingInstance.getId(),
        bindingInstance.getParentId().get());
  }

  @Test
  public void testDeleteById_ServiceWithoutObjectExsists_returnsOptionalEmpty() throws Exception {
    when(brokerSqlClient.selectInstance(serviceInstance.getId())).thenReturn(Optional.empty());
    assertFalse(store.deleteById(serviceInstance).isPresent());
  }

  @Test
  public void testDeleteById_SqlThrowsException_returnsOptionalEmpty() throws Exception {
    when(brokerSqlClient.selectInstance(serviceInstance.getId())).thenThrow(new SQLException());
    store.deleteById(serviceInstance);
  }
}
