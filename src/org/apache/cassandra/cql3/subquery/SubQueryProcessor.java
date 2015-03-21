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
import java.nio.charset.CharacterCodingException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.ThriftClientState;
import org.apache.cassandra.thrift.ThriftConversion;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.tracing.Tracing;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubQueryProcessor
{
    private static final Logger logger = LoggerFactory
            .getLogger(SubQueryProcessor.class);

    private static HashMap<Integer, SubQuery> preparedSubqery = new HashMap<>(1);

    public SubQuery getSlipttedQueries(String queryString, int level)
    {
        SubQuery sq = new SubQuery();

        int subQueryStartIndex = 0;
        int subQueryEndIndex = 0;

        String tempQueryString = queryString;
        String childQuery = "";

        if (level == 1)
            sq.setLast(true);

        int length = (StringUtils.countMatches(tempQueryString,
                "SELECT")) + (StringUtils.countMatches(tempQueryString,
                "select"));

        if (length > 1)
        {
            boolean isContainSubQuery = true;

            while (isContainSubQuery)
            {
                // All child subqueries will be created inside this loop

                subQueryStartIndex = findStartIndex(subQueryStartIndex,
                        tempQueryString);

                subQueryEndIndex = findEndIndex(subQueryStartIndex,
                        subQueryEndIndex, tempQueryString);

                childQuery = tempQueryString.substring(subQueryStartIndex,
                        subQueryEndIndex - 1).trim();

                sq.getSubQuery().add(getSlipttedQueries(childQuery, 0));

                tempQueryString = replaceString(tempQueryString,
                        subQueryStartIndex, subQueryEndIndex - 1);

                length = (StringUtils.countMatches(tempQueryString,
                        "SELECT")) + (StringUtils.countMatches(tempQueryString,
                        "select"));
                if (length == 1)
                    isContainSubQuery = false;

            }
            sq.setQuery(tempQueryString);

        }
        else
        {
            sq.setQuery(queryString);
        }
        return sq;
    }

    public int findEndIndex(int subQueryStartIndex, int subQueryEndIndex,
            String tempQueryString)
    {
        int end = 0;

        subQueryEndIndex = tempQueryString.indexOf(")", subQueryStartIndex) + 1;

        while (end == 0)
        {
            if (subQueryEndIndex < tempQueryString.length() - 1)
            {
                char c = tempQueryString.charAt(subQueryEndIndex);

                if ((c != ')'))
                {
                    if ((c + "").equals(" "))
                        subQueryEndIndex++;
                    end = 1;
                }
                else
                    subQueryEndIndex++;

            }
            else
                end = 1;

        }
        return subQueryEndIndex;

    }

    public int findStartIndex(int subQueryStartIndex, String tempQueryString)
    {
        subQueryStartIndex = tempQueryString.indexOf("(") + 1;
        int end = 0;
        while (end == 0)
        {
            char c = tempQueryString.charAt(subQueryStartIndex);
            if (c != '#')
            {
                end = 1;
            }
            else
            {
                subQueryStartIndex = tempQueryString.indexOf("(",
                        subQueryStartIndex) + 1;

            }
        }
        return subQueryStartIndex;

    }

    public String replaceString(String string, int subQueryStartsIndex,
            int subQueryEndsIndex)
    {

        String a = string.substring(0, subQueryStartsIndex);
        String b = string.substring(subQueryEndsIndex, string.length());

        int i = a.lastIndexOf("=");
        int j = b.lastIndexOf("in");
        if (j == 0)
        {
            j = b.lastIndexOf("IN");
        }

        if (i > j)
        {
            a = a.substring(0, i + 1);
            b = b.substring(1, b.length());
        }

        return a.trim() + "#" + b.trim();
    }

    public String executeChildQueryAndFillParent(SubQuery sq,
            ConsistencyLevel cLevel, ThriftClientState cState)
            throws InvalidRequestException, UnavailableException,
            TimedOutException
    {

        String queryString = sq.getQuery();
        List<SubQuery> subQueryList = sq.getSubQuery();

        if (subQueryList != null)
        {
            for (SubQuery subQuery : subQueryList)
            {
                queryString = queryString
                        .replaceFirst(
                                "#",
                                executeChildQueryAndFillParent(subQuery,
                                        cLevel, cState));
            }

        }

        if (sq.isLast() == false)
            return execute(queryString, cLevel, cState); // Execute and return
                                                         // value as string
        else
            return queryString;// return final query

    }

    public String execute(String queryString, ConsistencyLevel cLevel,
            ThriftClientState cState) throws UnavailableException,
            TimedOutException, InvalidRequestException
    {

        List<TypedColumn> values = new ArrayList<TypedColumn>();
        String valueString = "";
        try
        {
            // Execute query
            CqlResult cr = cState
                    .getCQLQueryHandler()
                    .process(
                            queryString,
                            cState.getQueryState(),
                            QueryOptions.fromProtocolV2(
                                    ThriftConversion.fromThrift(cLevel),
                                    Collections.<ByteBuffer> emptyList()))
                    .toThriftResult();

            Iterator<CqlRow> rowsIterator = cr.getRowsIterator();
            populateColumns(rowsIterator, values, cr.schema, queryString);

            AbstractJdbcType absJdbcType;
            int typeNo;
            for (TypedColumn tc : values)
            {

                absJdbcType = tc.getValueType();
                typeNo = absJdbcType.getJdbcType();
                if (typeNo == Types.INTEGER || typeNo == Types.BIGINT
                        || typeNo == Types.DECIMAL || typeNo == Types.FLOAT
                        || typeNo == Types.NUMERIC || typeNo == Types.DOUBLE)
                {
                    valueString = valueString + tc.getValueString() + ",";
                }
                else
                {
                    /**
                     * Values apart from Integer,Decimal,Float are need to be wrapped with single quotes(' ')[This is
                     * from the assumption that, the column type of parent query is same as reference column selected by
                     * child query]
                     **/
                    valueString = valueString + "'" + tc.getValueString()
                            + "',";
                }
            }
            if (valueString.contains(","))
            {
                valueString = valueString
                        .substring(0, valueString.lastIndexOf(','));
            }
            else

            {
                // Return error with query string if no value returned
                throw new InvalidRequestException("SubQuery is not returning any values"
                        + " for the query [" + queryString.toUpperCase() + "] ");
            }

        }
        catch (RequestExecutionException e)
        {
            ThriftConversion.rethrow(e);
            return null;
        }
        catch (RequestValidationException e)
        {
            // Return error with query string
            throw new InvalidRequestException(e.getMessage()
                    + " for the query [" + queryString.toUpperCase() + "] ");

        }
        catch (CharacterCodingException e)
        {
            throw new InvalidRequestException(e.getMessage()
                    + " for the query [" + queryString.toUpperCase() + "] ");

        }

        return valueString;
    }

    public void populateColumns(Iterator<CqlRow> rowsIterator,
            List<TypedColumn> values, CqlMetadata schema, String queryString)
            throws InvalidRequestException, CharacterCodingException
    {
        TypedColumn c;
        int i = 0;
        CqlRow row;
        List<Column> cols;
        while (rowsIterator.hasNext())
        {
            row = rowsIterator.next();
            cols = row.getColumns();
            // loop through the columns [For sub query the child queries only
            // have to select single column]
            i = 0;
            for (Column col : cols)
            {
                i++;
                c = createColumn(col, schema);
                values.add(c);
            }
            if (i > 1)
            {
                throw new InvalidRequestException(
                        "The child query ["
                                + queryString
                                + "] selected more than one columns,For sub query the child queries only have to select single column");
            }
        }

    }

    public TypedColumn createColumn(Column column, CqlMetadata schema)
            throws CharacterCodingException
    {

        String nameType = schema.name_types.get(column.name);
        if (nameType == null)
            nameType = "AsciiType";

        String valueType = schema.value_types.get(column.name);
        TypedColumn tc = new TypedColumn(column, valueType);

        return tc;
    }

    public String prepareStatementForChildQueries(SubQuery sq,
            ThriftClientState cState) throws RequestValidationException
    {

        String queryString = sq.getQueryWithMarker();
        List<SubQuery> subQueryList = sq.getSubQuery();

        for (SubQuery subQuery : subQueryList)
        {
            CqlPreparedResult cqlPreparedResult = cState
                    .getCQLQueryHandler()
                    .prepare(subQuery.getQueryWithMarker(),
                            cState.getQueryState()).toThriftPreparedResult();
            subQuery.setItemId(cqlPreparedResult.itemId);
            prepareStatementForChildQueries(subQuery, cState);
        }

        if (sq.isLast() == true)
            return queryString;
        else
        {
            return null;
        }
    }

    public synchronized static int getPreparedSubqerySize()
    {
        return SubQueryProcessor.preparedSubqery.size();
    }

    // Used for eviction
    public synchronized static void removeOneEntryFromPreparedSubqery()
    {
        if (SubQueryProcessor.preparedSubqery.size() > 1000)
        {
            int anyKey = SubQueryProcessor.preparedSubqery
                    .entrySet().iterator().next().getKey();
            SubQueryProcessor.preparedSubqery.remove(anyKey);
        }

    }

    public synchronized static SubQuery getPreparedSubqery(int itemId)
    {
        return SubQueryProcessor.preparedSubqery.get(itemId);
    }

    public synchronized static void putPreparedSubqery(int index, SubQuery preparedSubqery)
    {
        SubQueryProcessor.preparedSubqery.put(index, preparedSubqery);
    }

    public synchronized static boolean isPreparedSubquery(int itemId)
    {
        return SubQueryProcessor.preparedSubqery.containsKey(itemId);
    }

    public void assignBindVariablesToSubquery(List<ByteBuffer> bindVariables,
            SubQuery sq)
    {

        List<ByteBuffer> tempList = new ArrayList<ByteBuffer>();
        String temp = sq.getQuery();
        int i = 0;
        while (temp.contains("?"))
        {
            temp = temp.replace("?", "#");
            tempList.add(bindVariables.get(i));
            bindVariables.remove(i);
        }
        sq.setBindVariables(tempList);

        for (SubQuery subQuery : sq.getSubQuery())
        {

            assignBindVariablesToSubquery(bindVariables, subQuery);
        }
    }

    public int setMarkerString(SubQuery sq)
    {
        int totalMarkerCont = 0;
        String queryString = sq.getQuery();
        totalMarkerCont = StringUtils.countMatches(queryString,
                "?");
        queryString = queryString.replaceAll("#", "?");
        sq.setQueryWithMarker(queryString);
        List<SubQuery> subQueryList = sq.getSubQuery();
        for (SubQuery subQuery : subQueryList)
        {
            totalMarkerCont = totalMarkerCont + setMarkerString(subQuery);
        }
        return totalMarkerCont;
    }

    public List<ByteBuffer> PreparedQuery_executeChilAndFillParent(SubQuery sq,
            ConsistencyLevel cLevel, ThriftClientState cState)
            throws InvalidRequestException, RequestExecutionException,
            RequestValidationException
    {

        List<SubQuery> subQueryList = sq.getSubQuery();
        List<List<ByteBuffer>> childReturedRowsAsListOfList = new ArrayList<List<ByteBuffer>>();
        List<ByteBuffer> tempList = new ArrayList<ByteBuffer>();

        if (subQueryList != null)
        {
            for (SubQuery subQuery : subQueryList)
            {

                tempList = PreparedQuery_executeChilAndFillParent(subQuery,
                        cLevel, cState);

                childReturedRowsAsListOfList.add(tempList);

            }
            sq.setChildReturedRowsAsList(childReturedRowsAsListOfList);

        }

        if (sq.isLast() == false)
            return executePreparedSubqery(sq, cLevel, cState); // Execute child

        else
            return null;

    }

    public List<ByteBuffer> executePreparedSubqery(SubQuery sq,
            ConsistencyLevel cLevel, ThriftClientState cState)
            throws InvalidRequestException, RequestExecutionException,
            RequestValidationException
    {

        List<ByteBuffer> queryResultList = new ArrayList<ByteBuffer>();
        List<ByteBuffer> bindValriables = new ArrayList<ByteBuffer>();
        List<ByteBuffer> bindValueAndChildQueryResultsTogether = new ArrayList<ByteBuffer>();
        List<List<ByteBuffer>> childReturedRowsAsListOfList = new ArrayList<List<ByteBuffer>>();

        bindValriables = sq.getBindVariables();
        childReturedRowsAsListOfList = sq.childReturedRowsAsList;
        int iteration = 1;

        if (childReturedRowsAsListOfList.size() != 0)
        {
            if (childReturedRowsAsListOfList.get(0).size() >= 0)
            {
                iteration = childReturedRowsAsListOfList.get(0).size();
            }
        }

        ParsedStatement.Prepared prepared = cState.getCQLQueryHandler()
                .getPreparedForThrift(sq.getItemId());
        if (prepared == null)
            throw new InvalidRequestException(
                    String.format(
                            "Prepared query with ID %d not found"
                                    + " (either the query was not prepared on this host (maybe the host has been restarted?)"
                                    + " or you have prepared too many queries and it has been evicted from the internal cache)",
                            sq.getItemId()));
        logger.trace("Retrieved prepared statement #{} with {} bind markers",
                sq.getItemId(), prepared.statement.getBoundTerms());

        for (int i = 0; i < iteration; i++)
        {

            bindValueAndChildQueryResultsTogether.clear();
            String tempQuery = sq.getQuery();
            String tempQueryWithMarker = sq.getQueryWithMarker();
            int childListChooser = 0;
            int bindVariableChooser = 0;
            while (tempQueryWithMarker.contains("?"))
            {
                if (tempQuery.charAt(tempQueryWithMarker.indexOf("?")) == '?')
                {
                    tempQueryWithMarker = StringUtils.replaceOnce(
                            tempQueryWithMarker, "?", "#");
                    bindValueAndChildQueryResultsTogether.add(bindValriables
                            .get(bindVariableChooser));

                }
                else
                {
                    bindValueAndChildQueryResultsTogether
                            .add(childReturedRowsAsListOfList.get(
                                    childListChooser).get(i));

                    tempQueryWithMarker = StringUtils.replaceOnce(
                            tempQueryWithMarker, "?", "#");
                    childListChooser++;
                }
            }

            CqlResult cqlResult = cState
                    .getCQLQueryHandler()
                    .processPrepared(
                            prepared.statement,
                            cState.getQueryState(),
                            QueryOptions.fromProtocolV2(
                                    ThriftConversion.fromThrift(cLevel),
                                    bindValueAndChildQueryResultsTogether))
                    .toThriftResult();
            List<CqlRow> cqlRows = cqlResult.getRows();
            for (CqlRow cqlRow : cqlRows)
            {
                queryResultList.add(ByteBuffer.wrap(cqlRow.getColumns().get(0)
                        .getValue()));
            }

        }

        return queryResultList;
    }

    public CqlResult executeParentQery(SubQuery sq, ConsistencyLevel cLevel,
            ThriftClientState cState) throws InvalidRequestException,
            RequestExecutionException, RequestValidationException
    {

        CqlResult cqlResult = null;
        List<CqlRow> allCqlRows = new ArrayList<CqlRow>();

        List<ByteBuffer> bindValriables = new ArrayList<ByteBuffer>();
        List<ByteBuffer> bindValueAndChildQueryResultsTogether = new ArrayList<ByteBuffer>();
        List<List<ByteBuffer>> childReturedRowsAsListOfList = new ArrayList<List<ByteBuffer>>();

        bindValriables = sq.getBindVariables();
        childReturedRowsAsListOfList = sq.childReturedRowsAsList;

        int iteration = 1;

        if (childReturedRowsAsListOfList.get(0).size() >= 0)
        {

            iteration = childReturedRowsAsListOfList.get(0).size();
        }

        ParsedStatement.Prepared prepared = cState.getCQLQueryHandler()
                .getPreparedForThrift(sq.getItemId());
        if (prepared == null)
            throw new InvalidRequestException(
                    String.format(
                            "Prepared query with ID %d not found"
                                    + " (either the query was not prepared on this host (maybe the host has been restarted?)"
                                    + " or you have prepared too many queries and it has been evicted from the internal cache)",
                            sq.getItemId()));
        logger.trace("Retrieved prepared statement #{} with {} bind markers",
                sq.getItemId(), prepared.statement.getBoundTerms());

        for (int i = 0; i < iteration; i++)
        {

            bindValueAndChildQueryResultsTogether.clear();
            String tempQuery = sq.getQuery();
            String tempQueryWithMarker = sq.getQueryWithMarker();
            int childListChooser = 0;
            while (tempQueryWithMarker.contains("?"))
            {
                if (tempQuery.charAt(tempQueryWithMarker.indexOf("?")) == '?')
                {
                    tempQueryWithMarker = StringUtils.replaceOnce(
                            tempQueryWithMarker, "?", "#");
                    bindValueAndChildQueryResultsTogether.add(bindValriables
                            .get(i));

                }
                else
                {
                    bindValueAndChildQueryResultsTogether
                            .add(childReturedRowsAsListOfList.get(
                                    childListChooser).get(i));

                    tempQueryWithMarker = StringUtils.replaceOnce(
                            tempQueryWithMarker, "?", "#");
                    childListChooser++;
                }
            }

            cqlResult = cState
                    .getCQLQueryHandler()
                    .processPrepared(
                            prepared.statement,
                            cState.getQueryState(),
                            QueryOptions.fromProtocolV2(
                                    ThriftConversion.fromThrift(cLevel),
                                    bindValueAndChildQueryResultsTogether))
                    .toThriftResult();
            List<CqlRow> cqlRows = cqlResult.getRows();
            for (CqlRow cqlRow : cqlRows)
            {
                allCqlRows.add(cqlRow);
            }

        }
        cqlResult.setRows(allCqlRows);
        return cqlResult;

    }

    public CqlResult process(String queryString, Compression compression, ConsistencyLevel cLevel,
            ThriftClientState cState) throws InvalidRequestException, UnavailableException, TimedOutException,
            RequestExecutionException, RequestValidationException
    {
        /**
         * Parse the query into sub queries. A class SubQuery is created for each query and it also contains its child
         * queries and an identifier for to be executed at last.
         **/
        SubQuery sq = this.getSlipttedQueries(queryString,
                1);
        /**
         * Execute all child queries and fill values to its parent query,the final query will be returned without
         * execute
         **/
        String finalQuery = this
                .executeChildQueryAndFillParent(sq, cLevel, cState);
        /** Final query goes here **/
        return cState
                .getCQLQueryHandler()
                .process(
                        finalQuery,
                        cState.getQueryState(),
                        QueryOptions.fromProtocolV2(
                                ThriftConversion.fromThrift(cLevel),
                                Collections.<ByteBuffer> emptyList()))
                .toThriftResult();

    }

    public CqlPreparedResult prepare(String queryString, ThriftClientState cState) throws RequestValidationException
    {
        /**
         * Parse the query into sub queries. A class SubQuery is created for each query and it also contains its child
         * queries and an identifier for to be executed at last.
         **/
        SubQuery sq = this.getSlipttedQueries(queryString,
                1);
        int totalMarkerCont = this.setMarkerString(sq);

        String parentQuery = this
                .prepareStatementForChildQueries(sq, cState);

        CqlPreparedResult cqlPreparedResult = cState
                .getCQLQueryHandler()
                .prepare(parentQuery, cState.getQueryState())
                .toThriftPreparedResult();
        sq.setItemId(cqlPreparedResult.getItemId());

        int uniqueHashCodeForSubQuery = (parentQuery + "PreparedSubQuery" + sq.subQuery.get(0).queryWithMarker)
                .hashCode();
        SubQueryProcessor.removeOneEntryFromPreparedSubqery();

        SubQueryProcessor.putPreparedSubqery(uniqueHashCodeForSubQuery,
                sq);
        cqlPreparedResult.setItemId(uniqueHashCodeForSubQuery);
        cqlPreparedResult.setCount(totalMarkerCont);
        return cqlPreparedResult;

    }

    public CqlResult processPrepared(int itemId, List<ByteBuffer> bindVariables, ConsistencyLevel cLevel,
            ThriftClientState cState) throws InvalidRequestException, RequestExecutionException,
            RequestValidationException
    {
        SubQuery sq = SubQueryProcessor.getPreparedSubqery(itemId);

        if (sq == null)
            throw new InvalidRequestException(
                    String.format(
                            "Prepared query with ID %d not found"
                                    + " (either the query was not prepared on this host (maybe the host has been restarted?)"
                                    + " or you have prepared too many queries and it has been evicted from the internal cache)",
                            itemId));

        this.assignBindVariablesToSubquery(bindVariables,
                sq);
        /**
         * Execute all child queries and fill values to its parent // query,the final query will be returned without
         * execute
         **/
        this.PreparedQuery_executeChilAndFillParent(sq,
                cLevel, cState);
        /* Execute parent query and return */
        ParsedStatement.Prepared prepared = cState.getCQLQueryHandler()
                .getPreparedForThrift(sq.getItemId());

        return this.executeParentQery(sq, cLevel, cState);

    }
}
