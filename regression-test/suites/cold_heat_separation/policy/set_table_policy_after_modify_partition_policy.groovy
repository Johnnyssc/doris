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

suite("set_table_policy_after_modify_partition_policy") {
    // 1. set table storage policy after modify partition storage policy
    String tableName = "test_set_policy_table"

    sql """
        CREATE TABLE `${tableName}` (
            `ddate` date NULL,
            `dhour` varchar(*) NULL,
            `server_time` datetime NULL,
            `log_type` varchar(*) NULL,
            `source_flag` varchar(*) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`ddate`)
        PARTITION BY RANGE(`ddate`)
        (PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),
        PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01')))
        DISTRIBUTED BY RANDOM BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

    String resource1 = "test_set_resource1"
    String resource2 = "test_set_resource2"

    sql """
        CREATE RESOURCE IF NOT EXISTS "${resource1}"
        PROPERTIES(
            "type"="s3",
            "AWS_REGION" = "bj",
            "AWS_ENDPOINT" = "bj.s3.comaaaa",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_SECRET_KEY" = "aaaa",
            "AWS_ACCESS_KEY" = "bbba",
            "AWS_BUCKET" = "test-bucket",
            "s3_validity_check" = "false"
        );
        """

    sql """
        CREATE RESOURCE IF NOT EXISTS "${resource2}"
        PROPERTIES(
            "type"="s3",
            "AWS_REGION" = "bj",
            "AWS_ENDPOINT" = "bj.s3.comaaaa",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_SECRET_KEY" = "aaaa",
            "AWS_ACCESS_KEY" = "bbba",
            "AWS_BUCKET" = "test-bucket",
            "s3_validity_check" = "false"
        );
        """

    String policy_name1 = "test_set_policy1"
    String policy_name2 = "test_set_policy2"

    sql """
            CREATE STORAGE POLICY IF NOT EXISTS `${policy_name1}`
            PROPERTIES(
            "storage_resource" = "${resource1}",
            "cooldown_datetime" = "2999-06-18 00:00:00"
            );
        """
    sql """
            CREATE STORAGE POLICY IF NOT EXISTS `${policy_name2}`
            PROPERTIES(
            "storage_resource" = "${resource2}",
            "cooldown_datetime" = "2999-06-18 00:00:00"
            );
        """

    // modify partition's storage policy
    String partitionName = "p202403"
    sql """
            ALTER TABLE `${tableName}` MODIFY PARTITION (${partitionName}) SET("storage_policy"="${policy_name1}");
        """

    // set table storage policy, with exception
    test {
        sql """
                ALTER TABLE `${tableName}` set ("storage_policy" = "${policy_name2}");
            """
        exception "but partition [${partitionName}] already has storage policy"
    }

    //clean resource
    sql""" DROP TABLE IF EXISTS ${tableName} """

}
