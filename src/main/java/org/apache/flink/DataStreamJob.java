/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment ste = StreamTableEnvironment.create(env);

		String createTable1 = "CREATE TABLE parts (\n" +
				"    `timestamp`        TIMESTAMP(3),\n" +
				"    `partid`           STRING,\n" +
				"    `cost`             INT,\n" +
				"    `region`           INT,\n" +
				"    WATERMARK FOR `timestamp` AS `timestamp`\n" +
				") WITH (\n" +
				"    'connector' = 'filesystem',\n" +
				"    'path' = 'file:////parts.json',\n" +
				"    'format' = 'json'\n" +
				");";
		ste.executeSql(createTable1);

		String createTable2 = "CREATE TABLE partners (\n" +
				"    `timestamp`        TIMESTAMP(3),\n" +
				"    `region`           INT,\n" +
				"    `partner`          STRING,\n" +
				"    WATERMARK FOR `timestamp` AS `timestamp`\n" +
				") WITH (\n" +
				"    'connector' = 'filesystem',\n" +
				"    'path' = 'file:///regions.json',\n" +
				"    'format' = 'json'\n" +
				");";
		ste.executeSql(createTable2);

		/*
		  This should work as per https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/queries/hints/#state-ttl-hints
		  but instead throws
		  Exception in thread "main" org.apache.flink.table.api.ValidationException:
		  The options of following hints cannot match the name of input tables or views: `partners, parts` in `STATE_TTL`
		 */
		ste.executeSql("select /*+ STATE_TTL('parts' = '2m', 'partners' = '5m') */ * FROM parts JOIN partners ON parts.region = partners.region").print();

	}
}
