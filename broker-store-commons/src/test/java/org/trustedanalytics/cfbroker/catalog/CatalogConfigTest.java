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

import org.cloudfoundry.community.servicebroker.model.Plan;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.trustedanalytics.cfbroker.catalog.utils.PlanCollectionAsserts.assertThatPlanListsAreEqual;

public class CatalogConfigTest {

    @Test
    public void plans_servicePlansGiven_shouldTransformToCfPlans() {
        String baseId = "BASE_ID";
        List<ServicePlan> inputPlans = Arrays.asList(
            new ServicePlan("id1", "name1", "desc1", true),
            new ServicePlan("id2", "name2", "desc2", false)
        );
        List<Plan> expectedOutput = Arrays.asList(
            new Plan("BASE_ID-id1", "name1", "desc1", null, true),
            new Plan("BASE_ID-id2", "name2", "desc2", null, false)
        );

        CatalogConfig catalogConfig = new CatalogConfig();
        catalogConfig.setBaseId(baseId);
        catalogConfig.setPlans(inputPlans);

        List<Plan> actualOutput = catalogConfig.plans();

        assertThatPlanListsAreEqual(actualOutput, expectedOutput);
    }
}
