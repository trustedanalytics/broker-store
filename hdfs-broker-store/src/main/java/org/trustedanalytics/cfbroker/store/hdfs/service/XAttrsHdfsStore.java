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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.cfbroker.store.api.BrokerStore;
import org.trustedanalytics.cfbroker.store.api.Location;
import org.trustedanalytics.cfbroker.store.hdfs.helper.DirHelper;
import org.trustedanalytics.cfbroker.store.helper.LoggerHelper;
import org.trustedanalytics.cfbroker.store.serialization.RepositoryDeserializer;
import org.trustedanalytics.cfbroker.store.serialization.RepositorySerializer;

import java.io.IOException;
import java.util.Optional;

public class XAttrsHdfsStore<T> implements BrokerStore<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(XAttrsHdfsStore.class);

    private final RepositorySerializer<T> serializer;

    private final RepositoryDeserializer<T> deserializer;

    private final HdfsClient hdfsClient;

    private final String attributeName;

    private final String metadataPath;

    public XAttrsHdfsStore(HdfsClient hdfsClient, RepositorySerializer<T> serializer,
        RepositoryDeserializer<T> deserializer, String attributeName, String metadataPath) throws IOException {

        this.serializer = serializer;
        this.deserializer = deserializer;
        this.hdfsClient = hdfsClient;
        this.attributeName = attributeName;
        this.metadataPath = metadataPath;
    }

    @Override
    public void save(Location location, T t) throws IOException {
        String path = getPath(location);
        LOGGER.info(LoggerHelper.getParamsAsString("Saving instance in directory", path));
        hdfsClient.createDir(path);
        hdfsClient.addPathAttr(path, attributeName, serializer.serialize(t));
    }

    @Override
    public Optional<T> getById(Location location) throws IOException {
        String path = getPath(location);
        LOGGER.info("getById(" + path + ")");
        Optional<byte[]> data = hdfsClient.getPathAttr(path, attributeName);
        return data.isPresent()
            ? Optional.of(deserializer.deserialize(data.get())) : Optional.empty();
    }

    @Override
    public Optional<T> deleteById(Location location) throws IOException {
        Optional<T> instance = getById(location);
        if (instance.isPresent()) {
            hdfsClient.deleteById(getPath(location));
        }
        return instance;
    }

    private String getPath(Location location) {
        return DirHelper.concat(metadataPath, location.getPath());
    }

}
