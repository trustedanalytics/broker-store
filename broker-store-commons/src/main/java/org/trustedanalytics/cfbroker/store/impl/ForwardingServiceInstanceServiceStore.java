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

import org.cloudfoundry.community.servicebroker.exception.ServiceBrokerException;
import org.cloudfoundry.community.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.cloudfoundry.community.servicebroker.exception.ServiceInstanceExistsException;
import org.cloudfoundry.community.servicebroker.exception.ServiceInstanceUpdateNotSupportedException;
import org.cloudfoundry.community.servicebroker.model.CreateServiceInstanceRequest;
import org.cloudfoundry.community.servicebroker.model.DeleteServiceInstanceRequest;
import org.cloudfoundry.community.servicebroker.model.ServiceInstance;
import org.cloudfoundry.community.servicebroker.model.UpdateServiceInstanceRequest;
import org.cloudfoundry.community.servicebroker.service.ServiceInstanceService;

public class ForwardingServiceInstanceServiceStore implements ServiceInstanceService {


    private final ServiceInstanceService delegate;

    public ForwardingServiceInstanceServiceStore(ServiceInstanceService delegate) {
        this.delegate = delegate;
    }

    @Override
    public ServiceInstance createServiceInstance(CreateServiceInstanceRequest request)
            throws ServiceInstanceExistsException, ServiceBrokerException {
        return delegate.createServiceInstance(request);
    }

    @Override
    public ServiceInstance getServiceInstance(String id) {
        return delegate.getServiceInstance(id);
    }

    @Override
    public ServiceInstance deleteServiceInstance(DeleteServiceInstanceRequest request)
            throws ServiceBrokerException {
        return delegate.deleteServiceInstance(request);
    }

    @Override
    public ServiceInstance updateServiceInstance(UpdateServiceInstanceRequest request)
            throws ServiceInstanceUpdateNotSupportedException, ServiceBrokerException,
            ServiceInstanceDoesNotExistException {
        return delegate.updateServiceInstance(request);
    }

}
