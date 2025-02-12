// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License") you may not use this file except in compliance
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
import org.apache.ranger.*
import org.apache.ranger.plugin.model.*

suite("test_ranger_resource_catalog", "p2,ranger,external") {
	def tokens = context.config.jdbcUrl.split('/')
	def defaultJdbcUrl=tokens[0] + "//" + tokens[2] + "/?"
	def checkCatalogAccess = {access, user, password, catalog, dbName, tableName ->
		connect("$user", "$password", "$defaultJdbcUrl") {
			def executeSqlWithLogging = { sqlStatement, errorMessage ->
			    try {
			        sql sqlStatement
			    } catch (Exception e) {
				    if(access == "allow") {
				        log.error("Error executing ${sqlStatement}: ${e.getMessage()}")
				        throw e
				    }
				    log.info("Error executing ${sqlStatement}: ${e.getMessage()}")
			    }
			}

			executeSqlWithLogging("""SWITCH ${catalog}""", "Error executing SWITCH")
			executeSqlWithLogging("""DROP DATABASE IF EXISTS ${dbName}""", "Error executing DROP DATABASE")
			executeSqlWithLogging("""CREATE DATABASE IF NOT EXISTS ${dbName}""", "Error executing CREATE DATABASE")
			executeSqlWithLogging("""
			    CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName}` (
			        id BIGINT,
			        username VARCHAR(20)
			    )
			    DISTRIBUTED BY HASH(id) BUCKETS 2
			    PROPERTIES (
			        "replication_num" = "1"
			    );
			""", "Error executing CREATE TABLE")
			executeSqlWithLogging("""INSERT INTO ${dbName}.${tableName} VALUES (1, 'test')""", "Error executing INSERT")
			executeSqlWithLogging("""SELECT * FROM ${dbName}.${tableName}""", "Error executing SELECT")
			executeSqlWithLogging("""ALTER TABLE ${dbName}.${tableName} ADD COLUMN age INT""", "Error executing ALTER TABLE")
			executeSqlWithLogging("""CREATE VIEW ${dbName}.test_view AS SELECT * FROM ${dbName}.${tableName}""", "Error executing CREATE VIEW")
			executeSqlWithLogging("""SELECT * FROM ${dbName}.test_view""", "Error executing SELECT VIEW")
			executeSqlWithLogging("""SHOW CREATE VIEW ${dbName}.test_view""", "Error executing SHOW CREATE VIEW")
			executeSqlWithLogging("""DROP DATABASE ${dbName}""", "Error executing DROP DATABASE")
		}
	}

	String enabled = context.config.otherConfigs.get("enableRangerTest")
	String rangerEndpoint = context.config.otherConfigs.get("rangerEndpoint")
	String rangerUser = context.config.otherConfigs.get("rangerUser")
	String rangerPassword = context.config.otherConfigs.get("rangerPassword")
	String rangerServiceDefName = context.config.otherConfigs.get("rangerServiceDefName")
	String rangerServiceName = context.config.otherConfigs.get("rangerServiceName")
	String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
	String HmsPort = context.config.otherConfigs.get("hive3HmsPort")
	String jdbcUrl = context.config.jdbcUrl + "&sessionVariables=return_object_data_as_binary=true"
	String jdbcUser = context.config.jdbcUser
	String jdbcPassword = context.config.jdbcPassword
	String s3Endpoint = getS3Endpoint()
	String bucket = getS3BucketName()
	String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"

	if (enabled != null && enabled.equalsIgnoreCase("true")) {
		String catalog1 = 'ranger_test_catalog_1'
		String catalog2 = 'ranger_test_catalog_2'
		// prepare catalog
		sql """DROP CATALOG IF EXISTS ${catalog1}"""
		sql """DROP CATALOG IF EXISTS ${catalog2}"""
//		sql """CREATE CATALOG `${catalog1}` PROPERTIES (
//			"type"="hms",
//			'hive.metastore.uris' = 'thrift://${externalEnvIp}:${HmsPort}'
//		)"""
//		sql """ CREATE CATALOG `${catalog2}` PROPERTIES (
//        "user" = "${jdbcUser}",
//        "type" = "jdbc",
//        "password" = "${jdbcPassword}",
//        "jdbc_url" = "${jdbcUrl}",
//        "driver_url" = "${driverUrl}",
//        "driver_class" = "com.mysql.cj.jdbc.Driver"
//        )"""
		// prepare doris user
		String user1 = 'ranger_test_catalog_user_1'
		String user2 = 'ranger_test_catalog_user_2'
		String user3 = 'ranger_test_catalog_user_3'
		String pwd = 'C123_567p'
		sql """DROP USER IF EXISTS ${user1}"""
		sql """DROP USER IF EXISTS ${user2}"""
		sql """DROP USER IF EXISTS ${user3}"""
		sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
		sql """CREATE USER '${user2}' IDENTIFIED BY '${pwd}'"""
		sql """CREATE USER '${user3}' IDENTIFIED BY '${pwd}'"""
		// prepare ranger user

		createRangerUser(user1, pwd, ["ROLE_USER"] as String[])

		// create policy
		RangerClient rangerClient = new RangerClient("http://${rangerEndpoint}", "simple", rangerUser, rangerPassword, null)
		Map<String, RangerPolicy.RangerPolicyResource> resource = new HashMap<>()
		resource.put("catalog", new RangerPolicy.RangerPolicyResource("internal"))
		RangerPolicy policy = new RangerPolicy()

		String policy1 = 'ranger_test_catalog_policy_1'
		policy.setService(rangerServiceName)
		policy.setName(policy1)
		policy.setResources(resource)
		RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem()
		policyItem.addUser(user1)

		List<RangerPolicy.RangerPolicyItemAccess> policyItemAccesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>()
		List<String> catlogPolicy = ["GRANT", "SELECT", "LOAD", "ALTER", "CREATE", "DROP", "SHOW_VIEW"]

		catlogPolicy.forEach {
			policyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess(it))
		}

		policyItem.setAccesses(policyItemAccesses)
		policy.addPolicyItem(policyItem)
		rangerClient.deletePolicy(rangerServiceName, policy1)
		RangerPolicy createdPolicy = rangerClient.createPolicy(policy)
		System.out.println("New Policy created with id: " + createdPolicy.getId())
		// sleep 6s to wait for ranger policy to take effect
		// ranger.plugin.doris.policy.pollIntervalMs is 5000ms in ranger-doris-security.xml
		sleep(6000)
		checkCatalogAccess("allow", user1, pwd, "internal", 'ranger_test_catalog_db_1', 'ranger_test_catalog_table_1')
		checkCatalogAccess("deny", user2, pwd, "internal", 'ranger_test_catalog_db_1', 'ranger_test_catalog_table_1')
		checkCatalogAccess("deny", user3, pwd, "internal", 'ranger_test_catalog_db_1', 'ranger_test_catalog_table_1')

	}
}