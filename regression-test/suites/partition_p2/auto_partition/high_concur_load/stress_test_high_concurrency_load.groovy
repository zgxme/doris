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

import groovy.io.FileType
import java.nio.file.Files
import java.nio.file.Paths
import java.net.URL
import java.io.File

suite("stress_test_high_concurrency_load", "p2,nonConcurrent") {

    sql """ADMIN SET FRONTEND CONFIG ('max_auto_partition_num' = '10000000')"""
    sql """ADMIN SET FRONTEND CONFIG ('enable_cloud_txn_lazy_commit' = 'true')"""

    // get doris-db from s3
    def dirPath = context.file.parent
    def fatherPath = context.file.parentFile.parentFile.getPath()
    def fileName = "doris-dbgen"
    def fileUrl = "${getS3Url()}/regression/doris-dbgen-23-10-18/doris-dbgen-23-10-20/doris-dbgen"
    def filePath = Paths.get(dirPath, fileName)
    if (!Files.exists(filePath)) {
        new URL(fileUrl).withInputStream { inputStream ->
            Files.copy(inputStream, filePath)
        }
        def file = new File(dirPath + "/" + fileName)
        file.setExecutable(true)
    }

    def data_count = 10
    def cur_rows = 100000

    // use doris-dbgen product data file
    def doris_dbgen_create_data = { db_name, tb_name, part_type ->
        def rows = cur_rows  // total rows to load
        def bulkSize = rows
        def tableName = tb_name

        def jdbcUrl = context.config.jdbcUrl
        def urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
        def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
        def sql_port
        if (urlWithoutSchema.indexOf("/") >= 0) {
            // e.g: jdbc:mysql://locahost:8080/?a=b
            sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
        } else {
            // e.g: jdbc:mysql://locahost:8080
            sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
        }
        String feHttpAddress = context.config.feHttpAddress
        def http_port = feHttpAddress.substring(feHttpAddress.indexOf(":") + 1)

        String realDb = db_name
        String user = context.config.jdbcUser
        String password = context.config.jdbcPassword

        for (int i = 0; i < data_count; i++) {
            def cm
            if (password) {
                cm = """${context.file.parent}/doris-dbgen gen --host ${sql_ip} --sql-port ${sql_port} --user ${user} --pass ${password} --database ${realDb} --table ${tableName} --rows ${rows} --bulk-size ${bulkSize} --http-port ${http_port} --config ${context.file.parent}/../doris_dbgen_conf/two_stream_load_conflict.yaml --save-to-dir ${context.file.parent}/${part_type}/${part_type}_${i}/"""
            } else {
                cm = """${context.file.parent}/doris-dbgen gen --host ${sql_ip} --sql-port ${sql_port} --user ${user} --database ${realDb} --table ${tableName} --rows ${rows} --bulk-size ${bulkSize} --http-port ${http_port} --config ${context.file.parent}/../doris_dbgen_conf/two_stream_load_conflict.yaml --save-to-dir ${context.file.parent}/${part_type}/${part_type}_${i}/"""
            }
            logger.info("command is: " + cm)
            def proc = cm.execute()
            def sout = new StringBuilder(), serr = new StringBuilder()
            proc.consumeProcessOutput(sout, serr)
            proc.waitForOrKill(7200000)
            logger.info("std out: " + sout + "std err: " + serr)
        }
    }

    def load_result
    def doris_dbgen_stream_load_data = { db_name, tb_name, part_type, i ->
        def list = []
        def dir = new File("""${context.file.parent}""" + "/" + part_type + "/" + part_type + "_" + i)
        dir.eachFileRecurse (FileType.FILES) { file ->
            list << file
        }
        logger.info(list[0].toString())

        streamLoad {
            db "${db_name}"
            table "${tb_name}"

            set 'column_separator', '|'

            file """${list[0].toString()}"""
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                if (json.Status.toLowerCase() != "success" || 0 != json.NumberFilteredRows) {
                    load_result = result
                }
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(0, json.NumberFilteredRows)
            }
        }
    }

    def data_delete = { part_type ->

        def sql_cm = """rm -rf ${context.file.parent}/${part_type}"""
        sql_cm.execute()
    }

    String db = context.config.getDbNameByFile(context.file)
    def database_name = db
    def tb_name2 = "small_data_high_concurrent_load_range"

    sql """create database if not exists ${database_name};"""
    sql """use ${database_name};"""
    sql """drop table if exists ${tb_name2};"""
    sql new File("""${context.file.parent}/../ddl/small_data_high_concurrrent_load.sql""").text

    data_delete("range")
    doris_dbgen_create_data(database_name, tb_name2, "range")

    def thread_thread_1000 = []
    thread_thread_1000.add(Thread.startDaemon {doris_dbgen_stream_load_data(database_name, tb_name2, "range", 0)})
    thread_thread_1000.add(Thread.startDaemon {doris_dbgen_stream_load_data(database_name, tb_name2, "range", 1)})
    thread_thread_1000.add(Thread.startDaemon {doris_dbgen_stream_load_data(database_name, tb_name2, "range", 2)})
    thread_thread_1000.add(Thread.startDaemon {doris_dbgen_stream_load_data(database_name, tb_name2, "range", 3)})
    thread_thread_1000.add(Thread.startDaemon {doris_dbgen_stream_load_data(database_name, tb_name2, "range", 4)})
    thread_thread_1000.add(Thread.startDaemon {doris_dbgen_stream_load_data(database_name, tb_name2, "range", 5)})
    thread_thread_1000.add(Thread.startDaemon {doris_dbgen_stream_load_data(database_name, tb_name2, "range", 6)})
    thread_thread_1000.add(Thread.startDaemon {doris_dbgen_stream_load_data(database_name, tb_name2, "range", 7)})
    thread_thread_1000.add(Thread.startDaemon {doris_dbgen_stream_load_data(database_name, tb_name2, "range", 8)})
    thread_thread_1000.add(Thread.startDaemon {doris_dbgen_stream_load_data(database_name, tb_name2, "range", 9)})

    for (Thread th in thread_thread_1000) {
        th.join()
    }

    if (load_result != null) {
        def json = parseJson(load_result)
        log.info("Stream load failed. ${load_result}".toString())
        assertEquals("success", json.Status.toLowerCase())
        assertEquals(0, json.NumberFilteredRows)
    }

    def row_count_range = sql """select count(*) from ${tb_name2};"""
    assertTrue(cur_rows * data_count == row_count_range[0][0], "${cur_rows * data_count}, ${row_count_range[0][0]}")
    def partition_res_range = sql """show partitions from ${tb_name2} order by PartitionName;"""
    for (int i = 0; i < partition_res_range.size(); i++) {
        for (int j = i+1; j < partition_res_range.size(); j++) {
            if (partition_res_range[i][6] == partition_res_range[j][6]) {
                assertTrue(false, "$i, $j")
            }
        }
    }
}
