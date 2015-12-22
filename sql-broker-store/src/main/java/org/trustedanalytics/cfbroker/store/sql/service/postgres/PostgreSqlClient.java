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
package org.trustedanalytics.cfbroker.store.sql.service.postgres;

import org.trustedanalytics.cfbroker.store.sql.service.SqlConnectionUtils;
import org.trustedanalytics.cfbroker.store.sql.service.SqlDatabaseClient;

import java.sql.SQLException;

public class PostgreSqlClient extends SqlDatabaseClient {

  private static final String REVOKE_ALL_ON_DATABASE = "REVOKE ALL ON DATABASE %s FROM PUBLIC";
  private static final String SET_DATABASE_OWNER = "ALTER DATABASE %s OWNER TO %s";
  private static final String CREATE_ROLE = "CREATE ROLE %s PASSWORD '%s' LOGIN";
  private static final String DROP_ROLE = "DROP ROLE %s";

  public PostgreSqlClient(SqlConnectionUtils connection) throws SQLException {
    super(connection);
  }

  @Override
  public void provisionBrokerDatabase(String id, String owner, String password)
      throws SQLException {
    createDatabase(id);
    createRole(id, owner, password);
    revokeAllOnDatabase(owner);
  }

  @Override
  public void deprovisionBrokerDatabase(String id) throws SQLException {
    dropDatabase(id);
    dropRole(id);
  }

  private void createRole(String name, String owner, String password) throws SQLException {
    sqlConnectionUtils
        .execStatement(sqlConnectionUtils.prepareDDLStatement(CREATE_ROLE, owner, password));
    sqlConnectionUtils
        .execStatement(sqlConnectionUtils.prepareDDLStatement(SET_DATABASE_OWNER, name, owner));
  }

  private void dropRole(String name) throws SQLException {
    sqlConnectionUtils.execStatement(sqlConnectionUtils.prepareDDLStatement(DROP_ROLE, name));
  }

  private void revokeAllOnDatabase(String name) throws SQLException {
    sqlConnectionUtils
        .execStatement(sqlConnectionUtils.prepareDDLStatement(REVOKE_ALL_ON_DATABASE, name));
  }

}
