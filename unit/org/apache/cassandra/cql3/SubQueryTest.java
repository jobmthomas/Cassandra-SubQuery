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
package org.apache.cassandra.cql3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SubQueryTest extends SchemaLoader {
	private static EmbeddedCassandraService cassandra;

	static Connection conn;
	static Statement stmt;

	@BeforeClass
	public static void setup() throws Exception {
		Schema.instance.clear();
		cassandra = new EmbeddedCassandraService();
		cassandra.start();

		Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
		String url = "jdbc:cassandra://localhost:"
				+ DatabaseDescriptor.getRpcPort();
		System.out.println(url);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		conn.close();
	}

	@Test
	public void subQueryTest() throws SQLException {

		int flag = 0;
		dropKeyspace(stmt);
		createKeyspace(stmt);
		useKeyspace(stmt);

		// int
		createAndInsertTableCustomer(stmt, 10);
		createAndInsertTableAddress(stmt, 10);

		subQueryTestInt(conn, 10, flag);

		if (flag == 1) {
			Assert.assertTrue(true);
		} else {
			Assert.assertFalse(false);
		}

		flag = 0;

		preparedSubQueryTestInt(conn, 10, flag);
		if (flag == 1) {
			Assert.assertTrue(true);
		} else {
			Assert.assertFalse(false);
		}
	}

	private static void createAndInsertTableAddress(Statement stmt, int j)
			throws SQLException {
		String createTable = "CREATE TABLE ADDRESS(ID int PRIMARY KEY,PLACE text,COUNTRY text,ZIP text)";
		stmt.execute(createTable);
		for (int i = 0; i < j; i++) {

			String insertInTo = "INSERT INTO ADDRESS (ID, PLACE,COUNTRY,ZIP) VALUES ("
					+ i + ",'KOCHI','INDIA','682" + i + "');";
			stmt.execute(insertInTo);

		}

	}

	private static void createAndInsertTableCustomer(Statement stmt, int j)
			throws SQLException {

		String createTable = "CREATE TABLE CUSTOMER(ID int PRIMARY KEY,FIRSTNAME text,ADDRESS_ID int);";

		stmt.execute(createTable);

		for (int i = 0; i < j; i++) {

			String insertInTo = "INSERT INTO CUSTOMER (ID, FIRSTNAME,ADDRESS_ID) VALUES ("
					+ i + ",'David'," + i + ");";
			stmt.execute(insertInTo);

		}

	}

	private static void dropKeyspace(Statement stmt) throws SQLException {
		String createKeySpace = "DROP KEYSPACE if exists testks ";

		stmt.execute(createKeySpace);

	}

	private static void useKeyspace(Statement stmt) throws SQLException {
		String useTest = "USE testks";
		stmt.execute(useTest);

	}

	private static void createKeyspace(Statement stmt) throws SQLException {
		String createKeySpace = "CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";

		stmt.execute(createKeySpace);
	}

	private static void preparedSubQueryTestInt(Connection conn, int j, int flag)
			throws SQLException {

		PreparedStatement ps = conn
				.prepareStatement("SELECT ID,PLACE,COUNTRY,ZIP FROM ADDRESS WHERE ID = (SELECT ADDRESS_ID FROM CUSTOMER WHERE ID=? )");
		for (int i = 0; i < j; i++) {

			ps.setInt(1, i);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {

				rs.getObject(1);
				rs.getObject(2);
				rs.getObject(3);
				rs.getObject(4);
				flag = 1;

			}
		}

	}

	private static void subQueryTestInt(Connection conn, int j, int flag)
			throws SQLException {

		Statement stmt = conn.createStatement();
		for (int i = 0; i < j; i++) {

			ResultSet rs = stmt
					.executeQuery("SELECT ID,PLACE,COUNTRY,ZIP FROM ADDRESS WHERE ID = (SELECT ADDRESS_ID FROM CUSTOMER WHERE ID="
							+ i + " )");
			while (rs.next()) {

				rs.getObject(1);
				rs.getObject(2);
				rs.getObject(3);
				rs.getObject(4);
				flag = 1;

			}
		}
	}
}
