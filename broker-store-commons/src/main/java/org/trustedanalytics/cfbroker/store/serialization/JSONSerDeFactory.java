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
package org.trustedanalytics.cfbroker.store.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONSerDeFactory implements SerDeFactory {

    private static final SerDeFactory instance = new JSONSerDeFactory();

    private static final ObjectMapper mapper = new ObjectMapper();

    private JSONSerDeFactory() {
    }

    public static SerDeFactory getInstance() {
        return instance;
    }

    @Override
    public <T> RepositorySerializer<T> getSerializer() {
        return mapper::writeValueAsBytes;
    }

    @Override
    public <T> RepositoryDeserializer<T> getDeserializer(Class<T> type) {
        return t -> mapper.readValue(t, type);
    }
}
