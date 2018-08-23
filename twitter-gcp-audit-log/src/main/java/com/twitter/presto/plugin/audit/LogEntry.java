/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.presto.plugin.audit;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * This is the log entry in the audit log. Extra info is added for analysis.
 */
public class LogEntry
{
    // TODO(hluo): group them into hierarchical structure.
    private final QueryInfo queryInfo;
    private final QueryStats queryStats;
    // failure info
    private final Optional<QueryErrorLog> queryFailureInfo;

    @JsonCreator
    public LogEntry(
            @JsonProperty("queryInfo") QueryInfo queryInfo,
            @JsonProperty("queryStats") QueryStats queryStats,
            @JsonProperty("queryFailureInfo") Optional<QueryErrorLog> queryFailureInfo)
    {
        this.queryInfo = requireNonNull(queryInfo, "queryInfo is null");
        this.queryStats = requireNonNull(queryStats, "queryStats is null");
        this.queryFailureInfo = requireNonNull(queryFailureInfo, "queryFailureInfo is null");
    }

    @JsonProperty
    public QueryInfo getQueryInfo()
    {
        return queryInfo;
    }

    @JsonProperty
    public QueryStats getQueryStats()
    {
        return queryStats;
    }

    @JsonProperty
    public Optional<QueryErrorLog> getQueryFailureInfo()
    {
        return queryFailureInfo;
    }

    public static LogEntry createLogEntry(QueryCompletedEvent completedEvent)
    {
        return new LogEntry(
                QueryInfo.createQueryInfo(completedEvent),
                QueryStats.createQueryStats(completedEvent),
                completedEvent.getFailureInfo().isPresent()
                        ? Optional.of(QueryErrorLog.createQueryFailureInfo(completedEvent.getFailureInfo().get()))
                        : Optional.empty());
    }

    // query info
    public static class QueryInfo
    {
        private final String queryId;
        private final String user;
        private final String serverVersion;
        private final String environment;
        private final Optional<String> catalog;
        private final Optional<String> schema;
        private final String query;

        @JsonCreator
        public QueryInfo(
                @JsonProperty("queryId") String queryId,
                @JsonProperty("user") String user,
                @JsonProperty("serverVersion") String serverVersion,
                @JsonProperty("environment") String environment,
                @JsonProperty("catalog") Optional<String> catalog,
                @JsonProperty("schema") Optional<String> schema,
                @JsonProperty("query") String query)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.user = requireNonNull(user, "user is null");
            this.serverVersion = requireNonNull(serverVersion, "serverVersion is null");
            this.environment = requireNonNull(environment, "environment is null");
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.schema = requireNonNull(schema, "schema is null");
            this.query = requireNonNull(query, "query is null");
        }

        @JsonProperty
        public String getQueryId()
        {
            return queryId;
        }

        @JsonProperty
        public String getUser()
        {
            return user;
        }

        @JsonProperty
        public String getServerVersion()
        {
            return serverVersion;
        }

        @JsonProperty
        public String getEnvironment()
        {
            return environment;
        }

        @JsonProperty
        public Optional<String> getCatalog()
        {
            return catalog;
        }

        @JsonProperty
        public Optional<String> getSchema()
        {
            return schema;
        }

        @JsonProperty
        public String getQuery()
        {
            return query;
        }

        public static QueryInfo createQueryInfo(QueryCompletedEvent event)
        {
            return new QueryInfo(
                    event.getMetadata().getQueryId(),
                    event.getContext().getUser(),
                    event.getContext().getServerVersion(),
                    event.getContext().getEnvironment(),
                    event.getContext().getCatalog(),
                    event.getContext().getSchema(),
                    event.getMetadata().getQuery());
        }
    }

    public static class QueryStats
    {
        // query stats
        private final long createTime;
        private final long executionStartTime;
        private final long endTime;
        private final long queuedTime;
        private final long totalBytes;
        private final long totalRows;
        private final long queryWallTime;
        private final long peakMemory;
        private final double cumulativeMemory;
        private final long cpuTime;

        @JsonCreator
        public QueryStats(
                @JsonProperty("createTime") long createTime,
                @JsonProperty("executionStartTime") long executionStartTime,
                @JsonProperty("endTime") long endTime,
                @JsonProperty("queuedTime") long queuedTime,
                @JsonProperty("totalBytes") long totalBytes,
                @JsonProperty("totalRows") long totalRows,
                @JsonProperty("queryWallTime") long queryWallTime,
                @JsonProperty("peakMemory") long peakMemory,
                @JsonProperty("cumulativeMemory") double cumulativeMemory,
                @JsonProperty("cpuTime") long cpuTime)
        {
            this.createTime = requireNonNull(createTime, "createTime is null");
            this.executionStartTime = requireNonNull(executionStartTime, "executionStartTime is null");
            this.endTime = requireNonNull(endTime, "endTime is null");
            this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
            this.totalBytes = requireNonNull(totalBytes, "totalBytes is null");
            this.totalRows = requireNonNull(totalRows, "totalRows is null");
            this.queryWallTime = requireNonNull(queryWallTime, "queryWallTime is null");
            this.peakMemory = requireNonNull(peakMemory, "peakMemory is null");
            this.cumulativeMemory = requireNonNull(cumulativeMemory, "cumulativeMemory is null");
            this.cpuTime = requireNonNull(cpuTime, "cpuTime is null");
        }

        @JsonProperty
        public long getCreateTime()
        {
            return createTime;
        }

        @JsonProperty
        public long getExecutionStartTime()
        {
            return executionStartTime;
        }

        @JsonProperty
        public long getEndTime()
        {
            return endTime;
        }

        @JsonProperty
        public long getQueuedTime()
        {
            return queuedTime;
        }

        @JsonProperty
        public long getTotalBytes()
        {
            return totalBytes;
        }

        @JsonProperty
        public long getTotalRows()
        {
            return totalRows;
        }

        @JsonProperty
        public long getQueryWallTime()
        {
            return queryWallTime;
        }

        @JsonProperty
        public long getPeakMemory()
        {
            return peakMemory;
        }

        @JsonProperty
        public double getCumulativeMemory()
        {
            return cumulativeMemory;
        }

        @JsonProperty
        public long getCpuTime()
        {
            return cpuTime;
        }

        public static QueryStats createQueryStats(QueryCompletedEvent event)
        {
            return new QueryStats(
                    event.getCreateTime().toEpochMilli(),
                    event.getExecutionStartTime().toEpochMilli(),
                    event.getEndTime().toEpochMilli(),
                    event.getStatistics().getQueuedTime().toMillis(),
                    event.getStatistics().getTotalBytes(),
                    event.getStatistics().getTotalRows(),
                    event.getStatistics().getWallTime().toMillis(),
                    event.getStatistics().getPeakTotalNonRevocableMemoryBytes(),
                    event.getStatistics().getCumulativeMemory(),
                    event.getStatistics().getCpuTime().toMillis());
        }
    }

    public static class QueryErrorLog
    {
        // error info
        private final int errorCodeId;
        private final String errorCodeName;
        private final Optional<String> failureType;
        private final Optional<String> failureMessage;
        private final Optional<String> failureTask;
        private final Optional<String> failureHost;
        private final String failures;

        @JsonCreator
        public QueryErrorLog(
                @JsonProperty("errorCodeId") int errorCodeId,
                @JsonProperty("errorCodeName") String errorCodeName,
                @JsonProperty("failureType") Optional<String> failureType,
                @JsonProperty("failureMessage") Optional<String> failureMessage,
                @JsonProperty("failureTask") Optional<String> failureTask,
                @JsonProperty("failureHost") Optional<String> failureHost,
                @JsonProperty("failures") String failures)
        {
            this.errorCodeId = requireNonNull(errorCodeId, "errorCodeId is null");
            this.errorCodeName = requireNonNull(errorCodeName, "errorCodeName is null");
            this.failureType = requireNonNull(failureType, "failureType is null");
            this.failureMessage = requireNonNull(failureMessage, "failureMessage is null");
            this.failureTask = requireNonNull(failureTask, "failureTask is null");
            this.failureHost = requireNonNull(failureHost, "failureHost is null");
            this.failures = requireNonNull(failures, "failures is null");
        }

        @JsonProperty
        public int getErrorCodeId()
        {
            return errorCodeId;
        }

        @JsonProperty
        public String getErrorCodeName()
        {
            return errorCodeName;
        }

        @JsonProperty
        public Optional<String> getFailureType()
        {
            return failureType;
        }

        @JsonProperty
        public Optional<String> getFailureMessage()
        {
            return failureMessage;
        }

        @JsonProperty
        public Optional<String> getFailureTask()
        {
            return failureTask;
        }

        @JsonProperty
        public Optional<String> getFailureHost()
        {
            return failureHost;
        }

        @JsonProperty
        public String getFailures()
        {
            return failures;
        }

        public static QueryErrorLog createQueryFailureInfo(QueryFailureInfo failureInfo)
        {
            return new QueryErrorLog(
                    failureInfo.getErrorCode().getCode(),
                    failureInfo.getErrorCode().getName(),
                    failureInfo.getFailureType(),
                    failureInfo.getFailureMessage(),
                    failureInfo.getFailureTask(),
                    failureInfo.getFailureHost(),
                    failureInfo.getFailuresJson());
        }
    }
}
