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

import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.thrift.Column;

public class TypedColumn {

	private Column rawColumn;

	private String valueString = null;
	private String valueType = null;

	public TypedColumn(Column column, String valueType)
			throws CharacterCodingException {
		rawColumn = column;
		this.valueType = valueType;
		if (valueType.equals("org.apache.cassandra.db.marshal.TimestampType")) {
			this.valueType = "org.apache.cassandra.db.marshal.DateType";
		}
		if (column.value == null || !column.value.hasRemaining()) {
			this.valueString = null;
		} else {
			this.valueString = TypesMap.getTypeForComparator(this.valueType)
					.getString(column.value);
		}
	}

	public Column getRawColumn() {
		return rawColumn;
	}

	public String getValueString() {

		return valueString;
	}

	public AbstractJdbcType getValueType() {
		return TypesMap.getTypeForComparator(valueType);
	}

}
