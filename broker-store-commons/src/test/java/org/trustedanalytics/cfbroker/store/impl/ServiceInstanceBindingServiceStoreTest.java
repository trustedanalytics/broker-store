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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.trustedanalytics.cfbroker.store.api.BrokerStore;
import org.trustedanalytics.cfbroker.store.api.Location;
import org.cloudfoundry.community.servicebroker.exception.ServiceBrokerException;
import org.cloudfoundry.community.servicebroker.exception.ServiceInstanceBindingExistsException;
import org.cloudfoundry.community.servicebroker.model.CreateServiceInstanceBindingRequest;
import org.cloudfoundry.community.servicebroker.model.CreateServiceInstanceRequest;
import org.cloudfoundry.community.servicebroker.model.DeleteServiceInstanceBindingRequest;
import org.cloudfoundry.community.servicebroker.model.ServiceInstance;
import org.cloudfoundry.community.servicebroker.model.ServiceInstanceBinding;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ServiceInstanceBindingServiceStoreTest {

    @Mock
    private BrokerStore<CreateServiceInstanceBindingRequest> store;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private ServiceInstanceBindingServiceStore service;

    @Before
    public void init() {
        service = new ServiceInstanceBindingServiceStore(store);
    }

    private ServiceInstanceBinding getServiceInstanceBinding(String id) {
        return new ServiceInstanceBinding(id, "serviceId", Collections.emptyMap(), null, "guid");
    }

    private ServiceInstance getServiceInstance(String id) {
        return new ServiceInstance(
            new CreateServiceInstanceRequest(id, "planId", "organizationGuid", "spaceGuid")
                .withServiceInstanceId("serviceInstanceId"));
    }

    private CreateServiceInstanceBindingRequest getRequest(String id) {
        return new CreateServiceInstanceBindingRequest(
            getServiceInstance(id).getServiceDefinitionId(), "planId", "appGuid");
    }

    @Test
    public void testCreateServiceInstanceBinding_success_saveAndReturnsNewServiceInstanceBindingWithCredentials()
        throws Exception {

        Location storingLocation = Location.newInstance("bindingId", "serviceInstanceId");
        when(store.getById(storingLocation)).thenReturn(Optional.empty());
        CreateServiceInstanceBindingRequest request = new CreateServiceInstanceBindingRequest(
            getServiceInstance("serviceId").getServiceDefinitionId(), "planId", "appGuid").
            withBindingId("bindingId").withServiceInstanceId("serviceInstanceId");

        ServiceInstanceBinding instance = service.createServiceInstanceBinding(request);

        verify(store).getById(storingLocation);
        verify(store).save(storingLocation, request);
    }

    @Test(expected = ServiceInstanceBindingExistsException.class)
    public void testCreateServiceInstanceBinding_instanceAlreadyExists_throwsException()
        throws Exception {
        when(store.getById(any())).thenReturn(Optional.of(getRequest("id")));
        CreateServiceInstanceBindingRequest request = new CreateServiceInstanceBindingRequest(
            getServiceInstance("serviceId").getServiceDefinitionId(), "planId", "appGuid").
            withBindingId("bindingId").withServiceInstanceId("serviceId");
        service.createServiceInstanceBinding(request);
    }

    @Test
    public void testCreateServiceInstanceBinding_storeThrowsIOException_throwsException()
        throws Exception {
        Location storingLocation = Location.newInstance("bindingId", "serviceInstanceId");
        when(store.getById(storingLocation)).thenReturn(Optional.empty());
        doThrow(new IOException()).when(store).save(any(), any());
        thrown.expect(isA(ServiceBrokerException.class));
        thrown.expectCause(isA(IOException.class));

        service.createServiceInstanceBinding(
            new CreateServiceInstanceBindingRequest(
                getServiceInstance("serviceId").getServiceDefinitionId(), "planId", "appGuid").
                withBindingId("bindingId").withServiceInstanceId("serviceInstanceId"));
    }

    @Test
    public void testDeleteServiceInstance_existingInstance_deletesAndReturnsInstance()
        throws Exception {

        ServiceInstance serviceInstance = getServiceInstance("serviceId");
        Optional<ServiceInstanceBinding> instance = Optional.of(getServiceInstanceBinding("bindingId"));
        Location storingLocation = Location.newInstance("bindingId", "serviceInstanceId");
        when(store.deleteById(storingLocation))
            .thenReturn(Optional.of(getRequest("bindingId")));

        assertThat(service.deleteServiceInstanceBinding(
            new DeleteServiceInstanceBindingRequest("bindingId", serviceInstance, "", "")).getId(),
            equalTo(instance.get().getId()));
        verify(store).deleteById(storingLocation);
    }

    @Test
    public void testDeleteServiceInstance_nonExistingInstance_returnsNull() throws Exception {
        ServiceInstance serviceInstance = getServiceInstance("serviceId");
        Location storingLocation = Location.newInstance("bindingId", "serviceInstanceId");
        when(store.deleteById(storingLocation))
            .thenReturn(Optional.empty());

        assertNull(service.deleteServiceInstanceBinding(
            new DeleteServiceInstanceBindingRequest("bindingId", serviceInstance, "", "")));
    }

    @Test
    public void testDeleteServiceInstance_storeThrowsIOException_throwsException()
        throws Exception {

        ServiceInstance serviceInstance = getServiceInstance("serviceId");
        Location storingLocation = Location.newInstance("bindingId", "serviceInstanceId");
        when(store.deleteById(storingLocation))
            .thenThrow(new IOException());
        thrown.expect(isA(ServiceBrokerException.class));
        thrown.expectCause(isA(IOException.class));

        service.deleteServiceInstanceBinding(
            new DeleteServiceInstanceBindingRequest("bindingId", serviceInstance, "", ""));
    }
}
