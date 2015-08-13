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
import org.trustedanalytics.cfbroker.store.helper.LoggerHelper;
import org.cloudfoundry.community.servicebroker.exception.ServiceBrokerException;
import org.cloudfoundry.community.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.cloudfoundry.community.servicebroker.exception.ServiceInstanceExistsException;
import org.cloudfoundry.community.servicebroker.exception.ServiceInstanceUpdateNotSupportedException;
import org.cloudfoundry.community.servicebroker.model.CreateServiceInstanceRequest;
import org.cloudfoundry.community.servicebroker.model.DeleteServiceInstanceRequest;
import org.cloudfoundry.community.servicebroker.model.ServiceInstance;
import org.cloudfoundry.community.servicebroker.model.UpdateServiceInstanceRequest;
import org.cloudfoundry.community.servicebroker.service.ServiceInstanceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class ServiceInstanceServiceStore implements ServiceInstanceService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceInstanceServiceStore.class);

    private final BrokerStore<ServiceInstance> store;

    public ServiceInstanceServiceStore(BrokerStore<ServiceInstance> store) {
        this.store = store;
    }

    @Override
    public ServiceInstance createServiceInstance(CreateServiceInstanceRequest request)
        throws ServiceInstanceExistsException, ServiceBrokerException {
        LOGGER.debug(LoggerHelper
            .getParamsAsString("createServiceInstance", request.getServiceDefinitionId(),
                    request.getServiceInstanceId(), request.getPlanId(), request.getOrganizationGuid(),
                    request.getSpaceGuid()));
        ServiceInstance instance;
        try {
            Location storingLocation = Location.newInstance(request.getServiceInstanceId());
            if (store.getById(storingLocation).isPresent()) {
                throw new ServiceInstanceExistsException(store.getById(storingLocation).get());
            }

            instance = new ServiceInstance(request);
            store.save(storingLocation, instance);

        } catch (IOException e) {
            throw new ServiceBrokerException(e.getMessage(), e);
        }
        return instance;
    }

    @Override
    public ServiceInstance getServiceInstance(String id) {
        LOGGER.debug(LoggerHelper.getParamsAsString("getServiceInstance", id));
        try {
            return store.getById(Location.newInstance(id)).orElse(null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ServiceInstance deleteServiceInstance(DeleteServiceInstanceRequest request)
        throws ServiceBrokerException {

        LOGGER.debug(LoggerHelper
            .getParamsAsString("deleteServiceInstance", request.getServiceInstanceId(),
                request.getServiceId(), request.getPlanId()));

        Optional<ServiceInstance> instance;
        try {
            Location storingLocation = Location.newInstance(request.getServiceInstanceId());
            instance = store.deleteById(storingLocation);
        } catch (IOException e) {
            throw new ServiceBrokerException(e.getMessage(), e);
        }
        return instance.orElse(null);
    }

    @Override
    public ServiceInstance updateServiceInstance(UpdateServiceInstanceRequest request)
        throws ServiceInstanceUpdateNotSupportedException, ServiceBrokerException,
        ServiceInstanceDoesNotExistException {

        LOGGER.debug(LoggerHelper
            .getParamsAsString("updateServiceInstance", request.getServiceInstanceId(),
                request.getPlanId()));

        throw new ServiceInstanceUpdateNotSupportedException("Plan change not supported, planId"
            + request.getPlanId());
    }


}
