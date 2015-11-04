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
package org.trustedanalytics.cfbroker.store.sql.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.cfbroker.store.api.BrokerStore;
import org.trustedanalytics.cfbroker.store.api.Location;
import org.trustedanalytics.cfbroker.store.serialization.RepositoryDeserializer;
import org.trustedanalytics.cfbroker.store.serialization.RepositorySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

public class SqlBrokerStore<T> implements BrokerStore<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlBrokerStore.class);

  private final BrokerSqlClient brokerSqlClient;

  private final RepositorySerializer<T> serializer;

  private final RepositoryDeserializer<T> deserializer;

  public SqlBrokerStore(BrokerSqlClient client, RepositorySerializer<T> serializer,
      RepositoryDeserializer<T> deserializer) {
    this.brokerSqlClient = client;
    this.serializer = serializer;
    this.deserializer = deserializer;
  }

  @Override
  public Optional<T> getById(Location location) throws IOException {
    LOGGER.info("getById(" + location.getId() + ")");
    Optional<byte[]> data;
    try {
      if (location.getParentId().isPresent()) {
        data = brokerSqlClient.selectBinding(location.getId(), location.getParentId().get());
      } else {
        data = brokerSqlClient.selectInstance(location.getId());
      }
    } catch (SQLException e) {
      LOGGER.info(e.getMessage(), e);
      return Optional.empty();
    }
    return data.isPresent() ? Optional.of(deserializer.deserialize(data.get())) : Optional.empty();
  }

  @Override
  public void save(Location location, T o) throws IOException {
    LOGGER.info("save(" + location.getId() + ")");
    try {
      if (location.getParentId().isPresent()) {
        brokerSqlClient.insertBinding(location.getId(), location.getParentId().get(),
            serializer.serialize(o));
      } else {
        brokerSqlClient.insertInstance(location.getId(), serializer.serialize(o));
      }
    } catch (SQLException e) {
      throw new IOException("Unable to insert service instance table", e);
    }
  }

  @Override
  public Optional<T> deleteById(Location location) throws IOException {
    LOGGER.info("deleteById(" + location.getId() + ")");

    Optional<T> instance = getById(location);
    if (instance.isPresent()) {
      try {
        if (location.getParentId().isPresent()) {
          brokerSqlClient.deleteBinding(location.getId(), location.getParentId().get());
        } else {
          brokerSqlClient.deleteInstance(location.getId());
        }
      } catch (SQLException e) {
        throw new IOException("Unable to detele service instance", e);
      }
    }
    return instance;
  }
}
