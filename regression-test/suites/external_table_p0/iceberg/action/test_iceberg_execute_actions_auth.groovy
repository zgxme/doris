// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_iceberg_execute_actions_auth", "p0,external,doris,external_docker,external_docker_doris,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_execute_actions_auth"
    String db_name = "test_db_execute_actions_auth"
    String table_name = "test_execute_actions_auth"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalog_name}"""
    sql """CREATE DATABASE IF NOT EXISTS ${db_name}"""
    sql """use ${db_name}"""
    sql """DROP TABLE IF EXISTS ${table_name}"""
    sql """
        CREATE TABLE ${table_name} (
            id BIGINT,
            name STRING
        ) ENGINE=iceberg
    """
    sql """INSERT INTO ${table_name} VALUES (1, 'v1')"""
    sql """INSERT INTO ${table_name} VALUES (2, 'v2')"""
    sql """INSERT INTO ${table_name} VALUES (3, 'v3')"""

    List<List<Object>> snapshotList = sql """
        SELECT snapshot_id FROM ${table_name}\$snapshots ORDER BY committed_at
    """
    assertTrue(snapshotList.size() == 3, "Expected exactly 3 snapshots for execute action auth test")
    String earliestSnapshotId = snapshotList[0][0].toString()
    String latestSnapshotId = snapshotList[2][0].toString()

    String noPrivUser = "test_iceberg_execute_action_no_priv_user"
    String selectOnlyUser = "test_iceberg_execute_action_select_only_user"
    String alterOnlyUser = "test_iceberg_execute_action_alter_only_user"
    String password = "C123_567p"

    def createUserWithClusterUsage = { userName ->
        try_sql("DROP USER '${userName}'@'%'")
        sql """CREATE USER '${userName}'@'%' IDENTIFIED BY '${password}'"""
        if (isCloudMode()) {
            def clusters = sql """SHOW CLUSTERS"""
            assertTrue(!clusters.isEmpty())
            def validCluster = clusters[0][0]
            sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${userName}'@'%'"""
        }
        sql """CREATE DATABASE IF NOT EXISTS internal.regression_test"""
        sql """GRANT SELECT_PRIV ON internal.regression_test.* TO '${userName}'@'%'"""
    }

    try {
        createUserWithClusterUsage(noPrivUser)
        createUserWithClusterUsage(selectOnlyUser)
        createUserWithClusterUsage(alterOnlyUser)

        sql """GRANT SELECT_PRIV ON ${catalog_name}.${db_name}.${table_name} TO '${selectOnlyUser}'@'%'"""
        sql """GRANT ALTER_PRIV ON ${catalog_name}.${db_name}.${table_name} TO '${alterOnlyUser}'@'%'"""

        connect(noPrivUser, password, context.config.jdbcUrl) {
            test {
                sql """
                    ALTER TABLE ${catalog_name}.${db_name}.${table_name}
                    EXECUTE rewrite_manifests()
                """
                exception "denied"
            }
            test {
                sql """
                    ALTER TABLE ${catalog_name}.${db_name}.${table_name}
                    EXECUTE rollback_to_snapshot("snapshot_id" = "${earliestSnapshotId}")
                """
                exception "denied"
            }
        }

        connect(selectOnlyUser, password, context.config.jdbcUrl) {
            def selectOnlyRows = sql """
                SELECT * FROM ${catalog_name}.${db_name}.${table_name} ORDER BY id
            """
            assertTrue(selectOnlyRows.size() == 3, "Expected SELECT-only user to read all rows")
            assertTrue(selectOnlyRows[0][0] == 1 && selectOnlyRows[2][0] == 3,
                "Expected SELECT-only user to observe the original current table state")
            test {
                sql """
                    ALTER TABLE ${catalog_name}.${db_name}.${table_name}
                    EXECUTE rewrite_manifests()
                """
                exception "denied"
            }
            test {
                sql """
                    ALTER TABLE ${catalog_name}.${db_name}.${table_name}
                    EXECUTE rollback_to_snapshot("snapshot_id" = "${earliestSnapshotId}")
                """
                exception "denied"
            }
        }

        def alterOnlyRewriteResult = null
        connect(alterOnlyUser, password, context.config.jdbcUrl) {
            test {
                sql """SELECT * FROM ${catalog_name}.${db_name}.${table_name} ORDER BY id"""
                exception "denied"
            }

            alterOnlyRewriteResult = sql """
                ALTER TABLE ${catalog_name}.${db_name}.${table_name}
                EXECUTE rewrite_manifests()
            """
            assertTrue(alterOnlyRewriteResult.size() == 1,
                "Expected rewrite_manifests to return one result row for ALTER-only user")
            assertTrue(alterOnlyRewriteResult[0].size() == 2,
                "Expected rewrite_manifests to return two columns for ALTER-only user")

            def rollbackResult = sql """
                ALTER TABLE ${catalog_name}.${db_name}.${table_name}
                EXECUTE rollback_to_snapshot("snapshot_id" = "${earliestSnapshotId}")
            """
            assertTrue(rollbackResult.size() == 1, "Expected rollback_to_snapshot to return one result row")
            assertTrue(rollbackResult[0][1].toString() == earliestSnapshotId,
                "Expected ALTER-only user rollback to move current snapshot to the target snapshot")
        }

        def rowsAfterAlterOnlyRollback = sql """SELECT * FROM ${table_name} ORDER BY id"""
        assertTrue(rowsAfterAlterOnlyRollback.size() == 1,
            "Expected ALTER-only user rollback to change the visible current table state")
        assertTrue(rowsAfterAlterOnlyRollback[0][0] == 1,
            "Expected ALTER-only user rollback to move the table back to the earliest snapshot")

        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE set_current_snapshot("snapshot_id" = "${latestSnapshotId}")
        """
        def rowsAfterRestore = sql """SELECT * FROM ${table_name} ORDER BY id"""
        assertTrue(rowsAfterRestore.size() == 3, "Expected restore to recover the latest table state")
        assertTrue(rowsAfterRestore[0][0] == 1 && rowsAfterRestore[2][0] == 3,
            "Expected restore to recover all three inserted rows")
    } finally {
        [noPrivUser, selectOnlyUser, alterOnlyUser].each { userName ->
            try_sql("DROP USER '${userName}'@'%'")
        }
    }
}
