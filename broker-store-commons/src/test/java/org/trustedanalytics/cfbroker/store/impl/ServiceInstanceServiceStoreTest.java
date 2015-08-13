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
package org.trustedanalytics.cfbroker.store.impl;

import org.trustedanalytics.cfbroker.store.api.BrokerStore;
import org.trustedanalytics.cfbroker.store.api.Location;
import org.cloudfoundry.community.servicebroker.exception.ServiceBrokerException;
import org.cloudfoundry.community.servicebroker.exception.ServiceInstanceExistsException;
import org.cloudfoundry.community.servicebroker.exception.ServiceInstanceUpdateNotSupportedException;
import org.cloudfoundry.community.servicebroker.model.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class ServiceInstanceServiceStoreTest {


    @Mock
    private BrokerStore<ServiceInstance> store;

    @InjectMocks
    private ServiceInstanceServiceStore service;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testCreateServiceInstance_success_saveAndReturnsNewServiceInstance()
            throws Exception {
        ServiceInstance instance = getServiceInstance("id");
        Location storingLocation = Location.newInstance("id");
        when(store.getById(storingLocation)).thenReturn(Optional.empty());
        service.createServiceInstance(new CreateServiceInstanceRequest(
                getServiceDefinition().getId(), instance.getPlanId(), instance.getOrganizationGuid(), instance.getSpaceGuid()).
                withServiceInstanceId(instance.getServiceInstanceId()).withServiceDefinition(getServiceDefinition()));
        verify(store).getById(storingLocation);
        // TODO: ServiceInstance doesn't override equals, so the following will not work
        // could be done using stubbing with callbacks and manually comparing all fields
        // verify(store).save(instance.getId(), instance);
        // verify(store).save(instance.getServiceInstanceId(), instance);
    }

    @Test(expected = ServiceInstanceExistsException.class)
    public void testCreateServiceInstance_instanceAlreadyExists_throwsException()
            throws Exception {
        ServiceInstance instance = getServiceInstance("id");
        Location storingLocation = Location.newInstance("id");
        when(store.getById(storingLocation)).thenReturn(Optional.of(instance));
        CreateServiceInstanceRequest request = new CreateServiceInstanceRequest(
                getServiceDefinition().getId(), instance.getPlanId(), instance.getOrganizationGuid(), instance.getSpaceGuid()).
                withServiceInstanceId(instance.getServiceInstanceId()).withServiceDefinition(getServiceDefinition());

        when(store.getById(storingLocation)).thenReturn(Optional.of(instance));
        service.createServiceInstance(request);
    }


    @Test
    public void testCreateServiceInstance_storeThrowsIOException_throwsException() throws Exception {
        ServiceInstance instance = getServiceInstance("id");
        Location storingLocation = Location.newInstance("id");
        doThrow(new IOException()).when(store).save(any(), any());
        CreateServiceInstanceRequest request = new CreateServiceInstanceRequest(
                getServiceDefinition().getId(), instance.getPlanId(), instance.getOrganizationGuid(), instance.getSpaceGuid()).
                withServiceInstanceId(instance.getServiceInstanceId()).withServiceDefinition(getServiceDefinition());

        when(store.getById(storingLocation)).thenReturn(Optional.empty());
        thrown.expect(isA(ServiceBrokerException.class));
        thrown.expectCause(isA(IOException.class));
        service.createServiceInstance(request);
    }

    private ServiceInstance getServiceInstance(String id) {
        return new ServiceInstance(
                new CreateServiceInstanceRequest(getServiceDefinition().getId(), "planId", "organizationGuid", "spaceGuid")
                        .withServiceInstanceId(id));
    }


    private ServiceDefinition getServiceDefinition() {
        return new ServiceDefinition("def", "name", "desc", true, Collections.emptyList());
    }

    @Test
    public void testGetServiceInstance_idExists_returnsServiceInstance() throws Exception {
        Location storingLocation = Location.newInstance("id");
        when(store.getById(storingLocation)).thenReturn(Optional.of(getServiceInstance("id")));
        assertThat(service.getServiceInstance("id").getServiceInstanceId(), equalTo("id"));
    }

    @Test
    public void testGetServiceInstance_nonexistentId_returnsNull() throws Exception {
        Location storingLocation = Location.newInstance("id");
        when(store.getById(storingLocation)).thenReturn(Optional.empty());
        assertNull(service.getServiceInstance("id"));
    }

    @Test
    public void testGetServiceInstance_storeThrowsIOException_throwsException() throws Exception {
        when(store.getById(any())).thenThrow(new IOException());
        thrown.expect(isA(RuntimeException.class));
        thrown.expectCause(isA(IOException.class));
        service.getServiceInstance("id");
    }


    @Test
    public void testDeleteServiceInstance_existingInstance_deletesAndReturnsInstance()
            throws Exception {
        Optional<ServiceInstance> instance = Optional.of(getServiceInstance("id"));
        Location storingLocation = Location.newInstance("id");
        when(store.deleteById(storingLocation)).thenReturn(instance);

        assertThat(service.deleteServiceInstance(new DeleteServiceInstanceRequest("id", "", "")), equalTo(instance.get()));
        verify(store).deleteById(storingLocation);
    }

    @Test
    public void testDeleteServiceInstance_nonExistingInstance_returnsNull() throws Exception {
        Location storingLocation = Location.newInstance("id");
        when(store.deleteById(storingLocation)).thenReturn(Optional.empty());
        assertNull(service.deleteServiceInstance(new DeleteServiceInstanceRequest("id", "", "")));
    }

    @Test
    public void testDeleteServiceInstance_storeThrowsIOException_throwsException() throws Exception {
        Location storingLocation = Location.newInstance("id");
        when(store.deleteById(storingLocation)).thenThrow(new IOException());
        thrown.expect(isA(ServiceBrokerException.class));
        thrown.expectCause(isA(IOException.class));
        service.deleteServiceInstance(new DeleteServiceInstanceRequest("id", "", ""));
    }

    @Test(expected = ServiceInstanceUpdateNotSupportedException.class)
    public void testUpdateServiceInstance_whateverState_throwsException() throws Exception {
        service.updateServiceInstance(new UpdateServiceInstanceRequest("notused"));
    }
}
