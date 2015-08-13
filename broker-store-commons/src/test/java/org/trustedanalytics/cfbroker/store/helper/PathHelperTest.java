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
package org.trustedanalytics.cfbroker.store.helper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
public class PathHelperTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {null, ""},
            {"", ""},
            {"/", ""},
            {"////////", ""},
            {"path", "/path"},
            {"/path", "/path"},
            {"///path", "/path"},
            {"path/", "/path"},
            {"path///", "/path"},
            {"/path/", "/path"},
            {"///path///", "/path"},
            {"path/other", "/path/other"},
            {"/path/other", "/path/other"},
            {"///path/other", "/path/other"},
            {"path/other/", "/path/other"},
            {"path/other///", "/path/other"},
            {"/path/other/", "/path/other"},
            {"///path/other///", "/path/other"}
        });
    }

    private String input;

    private String expectedOutput;

    public PathHelperTest(String input, String expectedOutput) {
        this.input = input;
        this.expectedOutput = expectedOutput;
    }

    @Test
    public void normalizePath() {
        String failureMsg = String.format("Test failed for input '%s'", input);
        assertThat(failureMsg, expectedOutput, equalTo(PathHelper.normalizePath(input)));
    }
}
