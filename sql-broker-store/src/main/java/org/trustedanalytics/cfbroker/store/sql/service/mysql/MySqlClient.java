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
package org.trustedanalytics.cfbroker.store.sql.service.mysql;

import org.trustedanalytics.cfbroker.store.sql.service.SqlConnectionUtils;
import org.trustedanalytics.cfbroker.store.sql.service.SqlDatabaseClient;

import java.sql.SQLException;

public class MySqlClient extends SqlDatabaseClient {

  private static final String GRANT_PRIVILIGES = "GRANT ALL ON %s.* to '%s'@'%%'";
  private static final String CREATE_USER = "CREATE USER %s IDENTIFIED BY '%s'";
  private static final String DROP_USER = "DROP USER %s";

  public MySqlClient(SqlConnectionUtils connection) throws SQLException {
    super(connection);
  }

  @Override
  public void provisionBrokerDatabase(String id, String owner, String password)
      throws SQLException {
    createDatabase(id);
    createUser(id, owner, password);
  }

  @Override
  public void deprovisionBrokerDatabase(String id) throws SQLException {
    dropUser(id);
    dropDatabase(id);
  }

  private void createUser(String name, String owner, String password) throws SQLException {
    sqlConnectionUtils
        .execStatement(sqlConnectionUtils.prepareDDLStatement(CREATE_USER, owner, password));
    sqlConnectionUtils
        .execStatement(sqlConnectionUtils.prepareDDLStatement(GRANT_PRIVILIGES, name, owner));
  }

  private void dropUser(String name) throws SQLException {
    sqlConnectionUtils.execStatement(sqlConnectionUtils.prepareDDLStatement(DROP_USER, name));
  }

}
