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

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;


public class SerDeTest {

    @Test
    public void testSerDe_SerializingObject_serializesToJsonAndBack() throws Exception {
        RepositorySerializer<String> serializer = JSONSerDeFactory.getInstance().getSerializer();
        String instance = "junit";
        byte[] serialized = serializer.serialize(instance);
        RepositoryDeserializer<String> deserializer =
                JSONSerDeFactory.getInstance().getDeserializer(String.class);
        String deserialized = deserializer.deserialize(serialized);
        assertThat("Deserialized instance differs from serialized", instance,
                equalTo(deserialized));
    }

}
