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
package org.trustedanalytics.cfbroker.store.api;

import org.trustedanalytics.cfbroker.store.helper.PathHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
public class LocationTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"path", null, "/path"},
            {"/path", null, "/path"},
            {"///path", null, "/path"},
            {"path/", null, "/path"},
            {"path///", null, "/path"},
            {"/path/", null, "/path"},
            {"///path///", null, "/path"},
            {"path", "parent", "/parent/path"},
            {"/path", "parent", "/parent/path"},
            {"///path", "parent", "/parent/path"},
            {"path/", "parent", "/parent/path"},
            {"path///", "parent", "/parent/path"},
            {"/path/", "parent", "/parent/path"},
            {"///path///", "parent", "/parent/path"},
            {"path", "parent", "/parent/path"},
            {"path", "parent/", "/parent/path"},
            {"path", "parent///", "/parent/path"},
            {"path", "/parent", "/parent/path"},
            {"path", "///parent", "/parent/path"},
            {"path", "/parent/", "/parent/path"},
            {"path", "///parent///", "/parent/path"},
        });
    }

    private String id;
    private String parentId;
    private String expectedOutput;

    public LocationTest(String id, String parentId, String expectedOutput) {
        this.id = id;
        this.parentId = parentId;
        this.expectedOutput = expectedOutput;
    }

    @Test
    public void getPath() {
        String failureMsg = String.format("Test failed for id='%s', parentId='%s'", id, parentId);

        assertThat(failureMsg, expectedOutput,
            equalTo(PathHelper.normalizePath(Location.newInstance(id, parentId).getPath())));
    }
}
