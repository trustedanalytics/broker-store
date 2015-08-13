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
import org.cloudfoundry.community.servicebroker.exception.ServiceInstanceBindingExistsException;
import org.cloudfoundry.community.servicebroker.model.CreateServiceInstanceBindingRequest;
import org.cloudfoundry.community.servicebroker.model.DeleteServiceInstanceBindingRequest;
import org.cloudfoundry.community.servicebroker.model.ServiceInstanceBinding;
import org.cloudfoundry.community.servicebroker.service.ServiceInstanceBindingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;


public class ServiceInstanceBindingServiceStore implements ServiceInstanceBindingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceInstanceBindingServiceStore.class);

    private final BrokerStore<CreateServiceInstanceBindingRequest> store;

    public ServiceInstanceBindingServiceStore(BrokerStore<CreateServiceInstanceBindingRequest> store) {
        this.store = store;
    }

    @Override
    public ServiceInstanceBinding createServiceInstanceBinding(CreateServiceInstanceBindingRequest request)
            throws ServiceInstanceBindingExistsException, ServiceBrokerException {

        LOGGER.debug(LoggerHelper.getParamsAsString("createServiceInstanceBinding", request.getBindingId(),
                request.getServiceInstanceId(), request.getPlanId(), request.getAppGuid()));
        try {
            Location storingLocation = Location.newInstance(request.getBindingId(),
                request.getServiceInstanceId());

            Optional<CreateServiceInstanceBindingRequest> binding = store.getById(storingLocation);
            if (binding.isPresent()) {
                throw new ServiceInstanceBindingExistsException(getServiceInstanceBinding(binding.get()));
            }

            store.save(storingLocation, request);
        } catch (IOException e) {
            throw new ServiceBrokerException(e.getMessage(), e);
        }

        return getServiceInstanceBinding(request);
    }

    @Override
    public ServiceInstanceBinding deleteServiceInstanceBinding(DeleteServiceInstanceBindingRequest deleteRequest)
            throws ServiceBrokerException {
        LOGGER.debug(LoggerHelper.getParamsAsString("deleteServiceInstanceBinding", deleteRequest.getBindingId(),
                deleteRequest.getInstance(), deleteRequest.getServiceId(), deleteRequest.getPlanId()));
        Optional<ServiceInstanceBinding> bindingInstance;
        try {
            Location storingLocation = Location.newInstance(deleteRequest.getBindingId(),
                deleteRequest.getInstance().getServiceInstanceId());

            bindingInstance = store.deleteById(storingLocation)
                    .map(createRequest -> rewriteMissingAttrs(createRequest, deleteRequest))
                    .map(this::getServiceInstanceBinding);

        } catch (IOException e) {
            throw new ServiceBrokerException(e.getMessage(), e);
        }
        return bindingInstance.orElse(null);
    }

    private ServiceInstanceBinding getServiceInstanceBinding(CreateServiceInstanceBindingRequest request) {
        return new ServiceInstanceBinding(request.getBindingId(), request.getServiceInstanceId(),
                Collections.emptyMap(), null, request.getAppGuid());

    }

    private CreateServiceInstanceBindingRequest rewriteMissingAttrs(CreateServiceInstanceBindingRequest createRequest,
                                                                    DeleteServiceInstanceBindingRequest deleteRequest) {
        return createRequest.withBindingId(deleteRequest.getBindingId()).
                withServiceInstanceId(deleteRequest.getInstance().getServiceInstanceId());
    }
}
