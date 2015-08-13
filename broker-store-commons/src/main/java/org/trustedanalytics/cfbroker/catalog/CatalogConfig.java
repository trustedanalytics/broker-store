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
package org.trustedanalytics.cfbroker.catalog;

import org.cloudfoundry.community.servicebroker.model.Catalog;
import org.cloudfoundry.community.servicebroker.model.Plan;
import org.cloudfoundry.community.servicebroker.model.ServiceDefinition;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

@Configuration
@ConfigurationProperties(prefix="cf.catalog")
public class CatalogConfig {

    private static final String SYSLOG_DRAIN = "syslog_drain";
    private static final String IMAGE_URL = "imageUrl";

    @NotNull private List<ServicePlan> plans;
    @NotNull private Metadata metadata;
    @NotNull private String serviceName;
    @NotNull private String serviceId;
    @NotNull private String serviceDescription;
    @NotNull private String baseId;

    public List<ServicePlan> getPlans(){
        return this.plans;
    }

    public void setPlans(List<ServicePlan> plans) {
        this.plans = plans;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public void setServiceDescription(String serviceDescription) {
        this.serviceDescription = serviceDescription;
    }

    public void setBaseId(String baseId) {
        this.baseId = baseId;
    }

    @Bean
    public Catalog catalog() {
        return new Catalog(Arrays.asList(new ServiceDefinition(
            serviceId,
            serviceName,
            serviceDescription,
            true,                           //bindable
            true,                           //plan updateable
            plans(),
            null,                           //tags
            serviceDefinitionMetadata(),
            requirements(),
            null                            //dashboardClient
        )));
    }

    List<Plan> plans() {
        plans.stream().forEach(ServicePlan::validate);
        return plans.stream()
            .map(plan -> new Plan(
                baseId + "-" + plan.getId(),
                plan.getName(),
                plan.getDescription(),
                null,                       //metadata
                plan.isFree()
            ))
            .collect(toList());
    }

    private Map<String, Object> serviceDefinitionMetadata() {
        Map<String,Object> serviceMetadata = new HashMap<>();
        serviceMetadata.put(IMAGE_URL, metadata.getImageUrl());
        return serviceMetadata;
    }

    private List<String> requirements() {
        return Arrays.asList(SYSLOG_DRAIN);
    }
}
