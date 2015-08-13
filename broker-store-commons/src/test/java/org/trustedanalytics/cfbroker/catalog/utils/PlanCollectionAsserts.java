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
package org.trustedanalytics.cfbroker.catalog.utils;

import org.cloudfoundry.community.servicebroker.model.Plan;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class PlanCollectionAsserts {

    public static void assertThatPlanListsAreEqual(List<Plan> actualPlans, List<Plan> expectedPlans) {
        assertThat(actualPlans.size(), equalTo(expectedPlans.size()));

        IntStream.range(0, actualPlans.size())
            .mapToObj(i -> new Pair<>(actualPlans.get(i), expectedPlans.get(i)))
            .forEach(pair -> {
                Plan actual = pair.getLeft();
                Plan expected = pair.getRight();
                assertThat(actual.getDescription(), equalTo(expected.getDescription()));
                assertThat(actual.getId(), equalTo(expected.getId()));
                assertThat(actual.getMetadata(), equalTo(expected.getMetadata()));
                assertThat(actual.getName(), equalTo(expected.getName()));
                assertThat(actual.isFree(), equalTo(expected.isFree()));
            });
    }

    private static class Pair<T> {

        private final T left;
        private final T right;

        private Pair(T left, T right) {
            this.left = left;
            this.right = right;
        }

        public T getLeft() {
            return left;
        }

        public T getRight() {
            return right;
        }
    }
}
