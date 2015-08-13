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
package org.trustedanalytics.cfbroker.catalog.integration;

import org.trustedanalytics.cfbroker.catalog.CatalogConfig;
import org.cloudfoundry.community.servicebroker.model.Catalog;
import org.cloudfoundry.community.servicebroker.model.Plan;
import org.cloudfoundry.community.servicebroker.model.ServiceDefinition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

import static org.trustedanalytics.cfbroker.catalog.utils.PlanCollectionAsserts.assertThatPlanListsAreEqual;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.emptyCollectionOf;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = {CatalogConfig.class, AppContext.class},
    initializers = ConfigFileApplicationContextInitializer.class)
@IntegrationTest
public class CatalogConfigIntegrationTest {

    @Autowired
    private Catalog catalog;

    @Test
    public void catalogConfig_configurationInApplicationYml_configLoaded() {

        List<ServiceDefinition> serviceDefinitions = catalog.getServiceDefinitions();
        assertThat(serviceDefinitions.size(), equalTo(1));

        ServiceDefinition serviceDefinition = serviceDefinitions.get(0);
        assertThat(serviceDefinition.getDashboardClient(), equalTo(null));
        assertThat(serviceDefinition.getDescription(), equalTo("fake_desc"));
        assertThat(serviceDefinition.getId(), equalTo("fake_id"));
        assertThat(serviceDefinition.getMetadata().size(), equalTo(1));
        assertThat(serviceDefinition.getMetadata().get("imageUrl"), equalTo("fake_image"));
        assertThat(serviceDefinition.getName(), equalTo("fake_name"));
        assertThatPlanListsAreEqual(serviceDefinition.getPlans(), Arrays.asList(
            new Plan("fake_baseID-fake_plan_id", "fake_name", "fake_desc", null, true),
            new Plan("fake_baseID-fake_plan_id2", "fake_name2", "fake_desc2", null, false)
        ));
        assertThat(serviceDefinition.getRequires(), equalTo(Arrays.asList("syslog_drain")));
        assertThat(serviceDefinition.getTags(), emptyCollectionOf(String.class));
        assertThat(serviceDefinition.isBindable(), equalTo(true));
        assertThat(serviceDefinition.isPlanUpdateable(), equalTo(true));
    }
}
