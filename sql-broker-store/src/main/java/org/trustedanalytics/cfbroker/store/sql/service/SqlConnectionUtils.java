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

import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SqlConnectionUtils {

  private static final String DATA_COLUMN = "data";

  private final Map<SqlQueries, PreparedStatement> statementMap;

  private final String connectionString;

  public SqlConnectionUtils(String connectionString) throws SQLException {
    this.connectionString = connectionString;
    this.statementMap = createStatementMap();
  }

  public void execStatement(String query) throws SQLException {
    try (Statement statement = getConnection().createStatement()) {
      statement.execute(query);
    }
  }

  public void execInsertService(SqlQueries statement, String id, byte[] data) throws SQLException {
    try (PreparedStatement preparedStatement = resolveStatement(statement)) {
      preparedStatement.setString(1, id);
      preparedStatement.setBytes(2, data);
      preparedStatement.executeUpdate();
    }
  }

  public void execInsertBinding(SqlQueries statement, String serviceId, String bindingId,
      byte[] data) throws SQLException {
    try (PreparedStatement preparedStatement = resolveStatement(statement)) {
      preparedStatement.setString(1, serviceId);
      preparedStatement.setString(2, bindingId);
      preparedStatement.setBytes(3, data);
      preparedStatement.executeUpdate();
    }
  }

  public byte[] execSelectObject(SqlQueries statement, List<String> parameters)
      throws SQLException {
    try (PreparedStatement preparedStatement = resolveStatement(statement)) {
      setStatementParameters(preparedStatement, parameters);
      try (ResultSet result = preparedStatement.executeQuery()) {
        byte[] data = null;
        if (result.next()) {
          data = result.getBytes(DATA_COLUMN);
        }
        return data;
      }
    }
  }

  public void execDeleteStatement(SqlQueries statement, List<String> parameters)
      throws SQLException {
    try (PreparedStatement preparedStatement = resolveStatement(statement)) {
      setStatementParameters(preparedStatement, parameters);
      preparedStatement.executeUpdate();
    }
  }

  public String prepareDDLStatement(String query, String... parameters) {
    return String.format(query, parameters);
  }

  private void setStatementParameters(PreparedStatement statement, List<String> parameters)
      throws SQLException {
    for (int i = 0; i < parameters.size(); i++) {
      statement.setString(i + 1, parameters.get(i));
    }
  }

  private Map<SqlQueries, PreparedStatement> createStatementMap() throws SQLException {
    Map<SqlQueries, PreparedStatement> map = new EnumMap<>(SqlQueries.class);
    for (SqlQueries query : SqlQueries.values()) {
      map.put(query, getConnection().prepareStatement(query.getQuery()));
    }
    return map;
  }

  private PreparedStatement resolveStatement(SqlQueries statement) {
    return Objects.requireNonNull(statementMap.get(statement), "Statement returns null: " + statement);
  }

  private Connection getConnection() throws SQLException {
    try (Connection connection = DriverManager.getConnection(connectionString)) {
      connection.setAutoCommit(true);
      return connection;
    }
  }
}
