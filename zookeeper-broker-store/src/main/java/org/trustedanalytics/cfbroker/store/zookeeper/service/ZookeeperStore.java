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
package org.trustedanalytics.cfbroker.store.zookeeper.service;

import org.trustedanalytics.cfbroker.store.api.BrokerStore;
import org.trustedanalytics.cfbroker.store.api.Location;
import org.trustedanalytics.cfbroker.store.serialization.RepositoryDeserializer;
import org.trustedanalytics.cfbroker.store.serialization.RepositorySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class ZookeeperStore<T> implements BrokerStore<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperStore.class);

    private final ZookeeperClient zookeeperClient;

    private final RepositorySerializer<T> serializer;

    private final RepositoryDeserializer<T> deserializer;

    public ZookeeperStore(ZookeeperClient zookeeperClient, RepositorySerializer<T> serializer,
        RepositoryDeserializer<T> deserializer) {

        this.zookeeperClient = zookeeperClient;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override public void save(Location location, T t) throws IOException {
        String path = location.getPath();
        LOGGER.info("save(" + path + ", " + t.toString() + ")");

        zookeeperClient.addZNode(path, serializer.serialize(t));
    }

    @Override public Optional<T> getById(Location location) throws IOException {
        String path = location.getPath();
        LOGGER.info("getById(" + path + ")");

        byte[] data;
        try {
            data = zookeeperClient.getZNode(path);
        } catch (IOException e) {
            LOGGER.debug(e.getMessage(), e);
            return Optional.empty();
        }
        return Optional.ofNullable(deserializer.deserialize(data));
    }

    @Override public Optional<T> deleteById(Location location) throws IOException {
        String path = location.getPath();
        LOGGER.info("deleteById(" + path + ")");

        Optional<T> instance = getById(location);
        if (instance.isPresent()) {
            zookeeperClient.deleteZNode(path);
        }
        return instance;
    }
}
