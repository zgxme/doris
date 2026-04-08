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

suite("test_iceberg_rewrite_data_files_fault_injection",
        "p0,external,doris,external_docker,external_docker_doris,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_rewrite_data_files_fault_injection"
    String db_name = "test_db_rewrite_data_files_fault"
    String table_name = "test_rewrite_data_files_fault_atomicity"
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
            category STRING,
            payload STRING
        ) ENGINE=iceberg
    """

    (1..24).each { batchId ->
        sql """
            INSERT INTO ${table_name} VALUES
            (${batchId}, 'fault', 'payload-${batchId}')
        """
    }

    def rowsBeforeRewrite = sql """SELECT * FROM ${table_name} ORDER BY id"""
    assertTrue(rowsBeforeRewrite.size() == 24, "Expected seed data to produce 24 rows before rewrite")

    def snapshotBeforeFailure = sql """
        SELECT snapshot_id FROM ${table_name}\$snapshots ORDER BY committed_at DESC LIMIT 1
    """
    def snapshotCountBeforeFailure = sql """SELECT COUNT(*) FROM ${table_name}\$snapshots"""
    def filesBeforeFailure = sql """SELECT COUNT(*) FROM ${table_name}\$files"""
    def rowsBeforeFailure = sql """SELECT COUNT(*) FROM ${table_name}"""

    assertTrue((filesBeforeFailure[0][0] as int) > 1,
        "Expected multiple files before fault injection rewrite")

    try {
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().enableDebugPointForAllBEs("StreamSinkFileWriter.finalize.finalize_failed")

        test {
            sql """
                ALTER TABLE ${catalog_name}.${db_name}.${table_name}
                EXECUTE rewrite_data_files(
                    "target-file-size-bytes" = "10485760",
                    "min-input-files" = "1",
                    "rewrite-all" = "true",
                    "max-file-group-size-bytes" = "1"
                )
            """
            exception "Rewrite data files failed"
        }
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    def snapshotAfterFailure = sql """
        SELECT snapshot_id FROM ${table_name}\$snapshots ORDER BY committed_at DESC LIMIT 1
    """
    def snapshotCountAfterFailure = sql """SELECT COUNT(*) FROM ${table_name}\$snapshots"""
    def filesAfterFailure = sql """SELECT COUNT(*) FROM ${table_name}\$files"""
    def rowsAfterFailure = sql """SELECT COUNT(*) FROM ${table_name}"""

    assertTrue(snapshotAfterFailure[0][0].toString() == snapshotBeforeFailure[0][0].toString(),
        "Expected rewrite failure to keep the latest committed snapshot unchanged")
    assertTrue((snapshotCountAfterFailure[0][0] as int) == (snapshotCountBeforeFailure[0][0] as int),
        "Expected rewrite failure to avoid creating a committed snapshot")
    assertTrue((filesAfterFailure[0][0] as int) == (filesBeforeFailure[0][0] as int),
        "Expected rewrite failure to keep metadata-visible file count unchanged")
    assertTrue((rowsAfterFailure[0][0] as int) == (rowsBeforeFailure[0][0] as int),
        "Expected rewrite failure to preserve row count")

    def rowsAfterFailureDetail = sql """SELECT * FROM ${table_name} ORDER BY id"""
    assertEquals(rowsBeforeRewrite, rowsAfterFailureDetail)

    def retryRewriteResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name}
        EXECUTE rewrite_data_files(
            "target-file-size-bytes" = "10485760",
            "min-input-files" = "1",
            "rewrite-all" = "true",
            "max-file-group-size-bytes" = "1"
        )
    """
    assertTrue(retryRewriteResult.size() == 1, "Expected retry rewrite to return one result row")
    assertTrue(retryRewriteResult[0].size() == 4, "Expected retry rewrite to return four result columns")
    assertTrue((retryRewriteResult[0][0] as int) > 0, "Expected retry rewrite to rewrite at least one file")

    def snapshotAfterRetry = sql """
        SELECT snapshot_id FROM ${table_name}\$snapshots ORDER BY committed_at DESC LIMIT 1
    """
    def filesAfterRetry = sql """SELECT COUNT(*) FROM ${table_name}\$files"""
    def rowsAfterRetry = sql """SELECT COUNT(*) FROM ${table_name}"""

    assertTrue(snapshotAfterRetry[0][0].toString() != snapshotBeforeFailure[0][0].toString(),
        "Expected retry rewrite to create a new committed snapshot after fault is cleared")
    assertTrue((filesAfterRetry[0][0] as int) <= (filesBeforeFailure[0][0] as int),
        "Expected retry rewrite to keep file count non-increasing after compaction")
    assertTrue((rowsAfterRetry[0][0] as int) == (rowsBeforeFailure[0][0] as int),
        "Expected retry rewrite to preserve row count")

    def rowsAfterRetryDetail = sql """SELECT * FROM ${table_name} ORDER BY id"""
    assertEquals(rowsBeforeRewrite, rowsAfterRetryDetail)

    def timeoutTableName = "test_rewrite_data_files_timeout_atomicity"
    sql """DROP TABLE IF EXISTS ${timeoutTableName}"""
    sql """
        CREATE TABLE ${timeoutTableName} (
            id BIGINT,
            category STRING,
            payload STRING
        ) ENGINE=iceberg
    """

    (1..24).each { batchId ->
        sql """
            INSERT INTO ${timeoutTableName} VALUES
            (${batchId}, 'timeout', 'payload-${batchId}')
        """
    }

    def timeoutRowsBeforeRewrite = sql """SELECT * FROM ${timeoutTableName} ORDER BY id"""
    def timeoutSnapshotBeforeFailure = sql """
        SELECT snapshot_id FROM ${timeoutTableName}\$snapshots ORDER BY committed_at DESC LIMIT 1
    """
    def timeoutSnapshotCountBeforeFailure = sql """SELECT COUNT(*) FROM ${timeoutTableName}\$snapshots"""
    def timeoutFilesBeforeFailure = sql """SELECT COUNT(*) FROM ${timeoutTableName}\$files"""
    def timeoutRowsBeforeFailure = sql """SELECT COUNT(*) FROM ${timeoutTableName}"""
    def originalInsertTimeout = sql """SHOW VARIABLES WHERE Variable_name = 'insert_timeout'"""

    try {
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().enableDebugPointForAllBEs("LoadStreamStub::close_wait.long_wait")
        sql """SET insert_timeout = 1"""

        test {
            sql """
                ALTER TABLE ${catalog_name}.${db_name}.${timeoutTableName}
                EXECUTE rewrite_data_files(
                    "target-file-size-bytes" = "10485760",
                    "min-input-files" = "1",
                    "rewrite-all" = "true",
                    "max-file-group-size-bytes" = "1"
                )
            """
            exception "Rewrite tasks did not complete within timeout"
        }
    } finally {
        sql """SET insert_timeout = ${originalInsertTimeout[0][1]}"""
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    def timeoutSnapshotAfterFailure = sql """
        SELECT snapshot_id FROM ${timeoutTableName}\$snapshots ORDER BY committed_at DESC LIMIT 1
    """
    def timeoutSnapshotCountAfterFailure = sql """SELECT COUNT(*) FROM ${timeoutTableName}\$snapshots"""
    def timeoutFilesAfterFailure = sql """SELECT COUNT(*) FROM ${timeoutTableName}\$files"""
    def timeoutRowsAfterFailure = sql """SELECT COUNT(*) FROM ${timeoutTableName}"""
    def timeoutRowsAfterFailureDetail = sql """SELECT * FROM ${timeoutTableName} ORDER BY id"""

    assertTrue(timeoutSnapshotAfterFailure[0][0].toString() == timeoutSnapshotBeforeFailure[0][0].toString(),
        "Expected timeout failure to keep the latest committed snapshot unchanged")
    assertTrue((timeoutSnapshotCountAfterFailure[0][0] as int) == (timeoutSnapshotCountBeforeFailure[0][0] as int),
        "Expected timeout failure to avoid creating a committed snapshot")
    assertTrue((timeoutFilesAfterFailure[0][0] as int) == (timeoutFilesBeforeFailure[0][0] as int),
        "Expected timeout failure to keep metadata-visible file count unchanged")
    assertTrue((timeoutRowsAfterFailure[0][0] as int) == (timeoutRowsBeforeFailure[0][0] as int),
        "Expected timeout failure to preserve row count")
    assertEquals(timeoutRowsBeforeRewrite, timeoutRowsAfterFailureDetail)
}
