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
package org.apache.cassandra.cql3.subquery;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SubQuery {

	boolean isLast = false;
	int itemId = 0;
	public String query;
	public String queryWithMarker;

	List<SubQuery> subQuery = new ArrayList<SubQuery>();
	List<ByteBuffer> bindVariables = new ArrayList<ByteBuffer>();
	List<List<ByteBuffer>> childReturedRowsAsList = new ArrayList<List<ByteBuffer>>();
	List<ByteBuffer> bindValueAndChildQueryResultsTogether = new ArrayList<ByteBuffer>();

	public List<ByteBuffer> getBindValueAndChildQueryResultsTogether() {
		return bindValueAndChildQueryResultsTogether;
	}

	public void setBindValueAndChildQueryResultsTogether(
			List<ByteBuffer> bindValueAndChildQueryResultsTogether) {
		this.bindValueAndChildQueryResultsTogether = bindValueAndChildQueryResultsTogether;
	}

	public boolean isLast() {
		return isLast;
	}

	public List<List<ByteBuffer>> getChildReturedRowsAsList() {
		return childReturedRowsAsList;
	}

	public void setChildReturedRowsAsList(
			List<List<ByteBuffer>> childReturedRowsAsList) {
		this.childReturedRowsAsList = childReturedRowsAsList;
	}

	public List<ByteBuffer> getBindVariables() {
		return bindVariables;
	}

	public void setBindVariables(List<ByteBuffer> bindVariables) {
		this.bindVariables = bindVariables;
	}

	public void setLast(boolean isLast) {
		this.isLast = isLast;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public List<SubQuery> getSubQuery() {
		return subQuery;
	}

	public void setSubQuery(List<SubQuery> subQuery) {
		this.subQuery = subQuery;
	}

	public String getQueryWithMarker() {
		return queryWithMarker;
	}

	public void setQueryWithMarker(String queryWithMarker) {
		this.queryWithMarker = queryWithMarker;
	}

	public void setItemId(int itemId) {
		this.itemId = itemId;
	}

	public int getItemId() {
		return this.itemId;
	}
}
