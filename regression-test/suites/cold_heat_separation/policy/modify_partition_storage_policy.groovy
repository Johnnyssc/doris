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

suite("modify_partition_storage_policy") {
    /*
     回归测试用例作用：验证一张表的partition的storage policy是否可以在基于不同resource的policy中进行切换，验证结果应该是不可以
     4. 修改该分区的storage policy为policy2，结果应当是抛出异常
     5. 再次比对fe和be的元数据，查看是否对齐，结果应当是和第三步一样，对齐
     */

    //  1. 创建一个非分区表
    String tblName = "test_modify_partition_storage_policy"

    String policy_name1 = "test_modify_table_partition_storage_policy1"
    String policy_name2 = "test_modify_table_partition_storage_policy2"


    sql """DROP TABLE IF EXISTS ${tblName} FORCE; """

    sql """
        CREATE TABLE `${tblName}`
        (
            k1 BIGINT,
            k2 LARGEINT,
            v1 VARCHAR(2048)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH (k1) BUCKETS 3
        PROPERTIES(
            "replication_num" = "1"
        );
    """

    //  2.创建Resource1、Resource2，并创建对应的policy1和policy2
    def storage_exist = { name ->
        def show_storage_policy = sql """
        SHOW STORAGE POLICY;
        """
        for (iter in show_storage_policy) {
            if (name == iter[0]) {
                return true;
            }
        }
        return false;
    }

    if (!storage_exist.call(policy_name1) && !storage_exist.call(policy_name2)) {
        def test_modify_table_partition_storage_resource1 = try_sql """
            CREATE RESOURCE IF NOT EXISTS "test_modify_table_partition_storage_resource1"
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

        def test_modify_table_partition_storage_resource2 = try_sql """
            CREATE RESOURCE IF NOT EXISTS "test_modify_table_partition_storage_resource2"
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

        def test_modify_table_partition_storage_policy1 = try_sql """
            CREATE STORAGE POLICY IF NOT EXISTS `${policy_name1}`
            PROPERTIES(
            "storage_resource" = "test_modify_table_partition_storage_resource1",
            "cooldown_datetime" = "2025-06-18 00:00:00"
            );
        """

        def test_modify_table_partition_storage_policy2 = try_sql """
            CREATE STORAGE POLICY IF NOT EXISTS `${policy_name2}`
            PROPERTIES(
            "storage_resource" = "test_modify_table_partition_storage_resource2",
            "cooldown_datetime" = "2026-06-18 00:00:00"
            );
        """

        assertEquals(storage_exist.call(policy_name1), true)
        assertEquals(storage_exist.call(policy_name2), true)
    }

//    3. 给当前partition添加storage policy, 并比对fe和be的元数据中Storage信息是否对齐
    def alter_table_modify_partition_storage_policy_result = try_sql """
        ALTER TABLE ${tblName} MODIFY PARTITION (${tblName}) SET("storage_policy"=`${policy_name1}`);
    """

    // 3.1 get storage policy from fe metadata
    def partitions = sql "show partitions from ${tblName}"

    for (par in partitions) {
        // 12列是storage policy
        assertTrue(par[12] == "${policy_name1}")
    }

    // 3.2 get storage policy from be metadata
    def tabletResult = sql """SHOW TABLETS FROM TABLE ${tblName} PARTITION ${tblName};"""

    assert (tabletResult.size() > 0)

    def tablet_metadata_url = tabletResult[0]["MetaUrl"]

    def (code, out, err) = curl("GET", tablet_metadata_url)

    logger.info(" get be meta data: code=" + code + ", out=" + out + ", err=" + err)

    assertEquals(code, 0)

    def json = parseJson(out)

    // assert is equal
    assertEquals(json.storage_policy_id, policy_name1)

    // 4. alter policy with different resources
    try {
        sql """
        ALTER TABLE ${tblName} MODIFY PARTITION (${tblName}) SET("storage_policy"=`${policy_name2}`);
    """
    }
    catch (Exception e) {
        logger.info(e.getMessage())
    }
    // 4.1 fe
    def partitionsAfterAlter = sql "show partitions from ${tblName}"

    for (par in partitionsAfterAlter) {
        // 12列是storage policy
        assertTrue(par[12] == "${policy_name1}")
    }

    // 4.2 be
    def tabletResultAfterAlter = sql """SHOW TABLETS FROM TABLE ${tblName} PARTITION ${tblName};"""

    assert (tabletResultAfterAlter.size() > 0)

    def tablet_metadata_url_after_alter = tabletResult[0]["MetaUrl"]

    def (code1, out1, err1) = curl("GET", tablet_metadata_url_after_alter)

    logger.info(" get be meta data: code=" + code + ", out=" + out + ", err=" + err)

    assertEquals(code, 0)

    def json1 = parseJson(out)

    // assert is equal
    assertEquals(json1.storage_policy_id, policy_name1)

}
