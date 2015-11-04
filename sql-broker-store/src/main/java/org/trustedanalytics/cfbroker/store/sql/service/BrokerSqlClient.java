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

import java.sql.SQLException;
import java.util.Optional;

public interface BrokerSqlClient {

  void provisionBrokerDatabase(String id, String owner, String password) throws SQLException;

  void deprovisionBrokerDatabase(String id) throws SQLException;

  void insertMetadata(String id, byte[] data) throws SQLException;

  void insertInstance(String id, byte[] data) throws SQLException;

  void insertBinding(String id, String instanceId, byte[] data) throws SQLException;

  void deleteMetadata(String id) throws SQLException;

  void deleteInstance(String id) throws SQLException;

  void deleteBinding(String id, String instanceId) throws SQLException;

  Optional<byte[]> selectMetadata(String id) throws SQLException;

  Optional<byte[]> selectInstance(String id) throws SQLException;

  Optional<byte[]> selectBinding(String id, String instanceId) throws SQLException;

  void createDatabase(String name) throws SQLException;

  void dropDatabase(String name) throws SQLException;
}
