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

import org.junit.Before;
import org.junit.Test;

public class ServicePlanTest {

    private ServicePlan servicePlan;

    @Before
    public void setup() {
        servicePlan = new ServicePlan();
        servicePlan.setDescription("description");
        servicePlan.setFree(true);
        servicePlan.setId("id");
        servicePlan.setName("name");
    }

    @Test
    public void validate_allParamsValid_executesWithoutException() throws Exception {
        servicePlan.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void validate_descriptionNull_throwsIllegalArgumentException() throws Exception {
        servicePlan.setDescription(null);
        servicePlan.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void validate_freeNull_throwsIllegalArgumentException() throws Exception {
        servicePlan.setFree(null);
        servicePlan.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void validate_idNull_throwsIllegalArgumentException() throws Exception {
        servicePlan.setId(null);
        servicePlan.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void validate_nameNull_throwsIllegalArgumentException() throws Exception {
        servicePlan.setName(null);
        servicePlan.validate();
    }
}
