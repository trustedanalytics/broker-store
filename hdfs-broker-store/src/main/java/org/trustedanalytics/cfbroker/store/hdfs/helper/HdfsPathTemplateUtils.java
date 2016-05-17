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
package org.trustedanalytics.cfbroker.store.hdfs.helper;

import org.apache.commons.lang.text.StrSubstitutor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class HdfsPathTemplateUtils {

    public static final String VARIABLE_PREFIX = "%{";
    public static final String VARIABLE_SUFIX = "}";

    private HdfsPathTemplateUtils() {}

    public static String fill(String path, UUID instanceId, UUID orgId) {
        return fill(path, Optional.ofNullable(instanceId).map(UUID::toString).orElse(null),
                Optional.ofNullable(orgId).map(UUID::toString).orElse(null));
    }

    public static String fill(String path, String instanceId, String orgId) {
        Map<String, String> values = new HashMap<>();
        values.put("instance", instanceId);
        values.put("organization", orgId);
        values = values.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        return getSubstitutor(values).replace(DirHelper.removeTrailingSlashes(path));
    }

    public static StrSubstitutor getSubstitutor(Map<String, String> values) {
        return new StrSubstitutor(values, VARIABLE_PREFIX, VARIABLE_SUFIX);
    }
}
