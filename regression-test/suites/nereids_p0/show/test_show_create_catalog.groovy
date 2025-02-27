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

suite("test_show_create_catalog", "p0,external,hive,external_docker,external_docker_hive") {

    String catalog_name = "es"

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
            "type"="es",
            "hosts"="http://127.0.0.1:9200"
    );"""

    checkNereidsExecute("""show create catalog ${catalog_name}""")
    qt_cmd("""show create catalog ${catalog_name}""")

    sql """drop catalog if exists ${catalog_name}"""
}
