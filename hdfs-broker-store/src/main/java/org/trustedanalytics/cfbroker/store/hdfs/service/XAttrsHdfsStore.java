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
package org.trustedanalytics.cfbroker.store.hdfs.service;

import org.trustedanalytics.cfbroker.store.api.BrokerStore;
import org.trustedanalytics.cfbroker.store.api.Location;
import org.trustedanalytics.cfbroker.store.serialization.RepositoryDeserializer;
import org.trustedanalytics.cfbroker.store.serialization.RepositorySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class XAttrsHdfsStore<T> implements BrokerStore<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(XAttrsHdfsStore.class);

    private final RepositorySerializer<T> serializer;

    private final RepositoryDeserializer<T> deserializer;

    private final HdfsClient hdfsClient;

    private final String attributeName;

    public XAttrsHdfsStore(HdfsClient hdfsClient, RepositorySerializer<T> serializer,
        RepositoryDeserializer<T> deserializer, String attributeName) {

        this.serializer = serializer;
        this.deserializer = deserializer;
        this.hdfsClient = hdfsClient;
        this.attributeName = attributeName;
    }

    @Override
    public void save(Location location, T t) throws IOException {
        String path = location.getPath();
        hdfsClient.createDir(path);
        hdfsClient.addPathAttr(path, attributeName, serializer.serialize(t));
    }

    @Override
    public Optional<T> getById(Location location) throws IOException {
        String path = location.getPath();
        LOGGER.info("getById(" + path + ")");
        Optional<byte[]> data = hdfsClient.getPathAttr(path, attributeName);
        return data.isPresent()
            ? Optional.of(deserializer.deserialize(data.get())) : Optional.empty();
    }

    @Override
    public Optional<T> deleteById(Location location) throws IOException {
        Optional<T> instance = getById(location);
        if (instance.isPresent()) {
            hdfsClient.deleteById(location.getPath());
        }
        return instance;
    }
}
