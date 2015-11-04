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
import java.util.Arrays;
import java.util.Optional;

public abstract class SqlDatabaseClient implements BrokerSqlClient {

  private static final String CREATE_DATABASE = "CREATE DATABASE %s";
  private static final String DROP_DATABASE = "DROP DATABASE %s";

  protected final SqlConnectionUtils sqlConnectionUtils;

  public SqlDatabaseClient(SqlConnectionUtils connection) throws SQLException {
    this.sqlConnectionUtils = connection;
  }

  @Override
  public Optional<byte[]> selectInstance(String id) throws SQLException {
    return Optional.ofNullable(
        sqlConnectionUtils.execSelectObject(SqlQueries.SELECT_INSTANCE, Arrays.asList(id)));
  }

  @Override
  public Optional<byte[]> selectBinding(String id, String instanceId) throws SQLException {
    return Optional.ofNullable(sqlConnectionUtils.execSelectObject(SqlQueries.SELECT_BINDING,
        Arrays.asList(id, instanceId)));
  }

  @Override
  public Optional<byte[]> selectMetadata(String id) throws SQLException {
    return Optional.ofNullable(
        sqlConnectionUtils.execSelectObject(SqlQueries.SELECT_METADATA, Arrays.asList(id)));
  }

  @Override
  public void insertMetadata(String id, byte[] data) throws SQLException {
    sqlConnectionUtils.execInsertService(SqlQueries.INSERT_METADATA, id, data);
  }

  @Override
  public void insertInstance(String id, byte[] data) throws SQLException {
    sqlConnectionUtils.execInsertService(SqlQueries.INSERT_INSTANCE, id, data);
  }

  @Override
  public void insertBinding(String id, String instanceId, byte[] data) throws SQLException {
    sqlConnectionUtils.execInsertBinding(SqlQueries.INSERT_BINDING, instanceId, id, data);
  }

  @Override
  public void deleteInstance(String id) throws SQLException {
    sqlConnectionUtils.execDeleteStatement(SqlQueries.DELETE_INSTANCE, Arrays.asList(id));
  }

  @Override
  public void deleteBinding(String id, String instanceId) throws SQLException {
    sqlConnectionUtils.execDeleteStatement(SqlQueries.DELETE_BINDING,
        Arrays.asList(id, instanceId));
  }

  @Override
  public void deleteMetadata(String id) throws SQLException {
    sqlConnectionUtils.execDeleteStatement(SqlQueries.DELETE_METADATA, Arrays.asList(id));
  }

  @Override
  public void createDatabase(String name) throws SQLException {
    sqlConnectionUtils.execStatement(
        sqlConnectionUtils.prepareDDLStatement(CREATE_DATABASE, name));
  }

  @Override
  public void dropDatabase(String name) throws SQLException {
    sqlConnectionUtils.execStatement(
        sqlConnectionUtils.prepareDDLStatement(DROP_DATABASE, name));
  }
}
