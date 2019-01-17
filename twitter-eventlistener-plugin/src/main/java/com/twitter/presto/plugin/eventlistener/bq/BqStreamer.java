package com.twitter.presto.plugin.eventlistener.bq;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Splitter;
import com.twitter.presto.plugin.eventlistener.TwitterEventHandler;
import com.twitter.presto.plugin.eventlistener.TwitterEventListenerConfig;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

import static com.google.common.base.Preconditions.checkArgument;

public class BqStreamer
        implements TwitterEventHandler
{
    private static final Logger log = Logger.get(BqStreamer.class);

    private final BigQuery bigquery;
    private final TableId tableId;

    @Inject
    public BqStreamer(TwitterEventListenerConfig config)
    {
        List<String> parts = Splitter.on('.').splitToList(config.getBqTableFullName());
        checkArgument(parts.size() == 3, "invalid fully qualified bigquery table name");
        String projectId = parts.get(0);
        String datasetName = parts.get(1);
        String tableName = parts.get(2);

        this.bigquery = BigQueryOptions.getDefaultInstance().getService();
        this.tableId = TableId.of(projectId, datasetName, tableName);
    }

    public void handleQueryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        Map<String, Object> rowContent = prepareRowContent(queryCompletedEvent);
        InsertAllResponse response =
                bigquery.insertAll(
                        InsertAllRequest.newBuilder(tableId)
                                .addRow(rowContent)
                                .build());
        if (response.hasErrors()) {
            log.warn("failed to write QueryCompletedEvent to BigQuery: " +
                    response.getInsertErrors().entrySet().stream()
                            .map(x -> x.toString())
                            .collect(toList())
                            .toString());
        }
    }

    private static Map<String, Object> prepareRowContent(QueryCompletedEvent queryCompletedEvent)
    {
        Map<String, Object> rowContent = new HashMap<>();
        // createTime, executionStartTime, endTime
        rowContent.put("create_time_ms", queryCompletedEvent.getCreateTime().toEpochMilli());
        rowContent.put("execution_start_time_ms", queryCompletedEvent.getExecutionStartTime().toEpochMilli());
        rowContent.put("end_time_ms", queryCompletedEvent.getEndTime().toEpochMilli());

        // QueryMetadata
        QueryMetadata queryMetadata = queryCompletedEvent.getMetadata();
        rowContent.put("query_id", queryMetadata.getQueryId());
        queryMetadata.getTransactionId().ifPresent(x -> rowContent.put("transaction_id", x));
        rowContent.put("query", queryMetadata.getQuery());
        rowContent.put("query_state", queryMetadata.getQueryState());
        rowContent.put("uri_", queryMetadata.getUri());
        queryMetadata.getPlan().ifPresent(x -> rowContent.put("plan", x));
        queryMetadata.getPayload().ifPresent(x -> rowContent.put("payload", x));

        // QueryStatistics
        QueryStatistics queryStatistics = queryCompletedEvent.getStatistics();
        rowContent.put("cpu_time_ms", queryStatistics.getCpuTime().toMillis());
        rowContent.put("wall_time_ms", queryStatistics.getWallTime().toMillis());
        rowContent.put("queued_time_ms", queryStatistics.getQueuedTime().toMillis());
        queryStatistics.getAnalysisTime().ifPresent(x -> rowContent.put("analysis_time_ms", x));
        queryStatistics.getDistributedPlanningTime().ifPresent(x -> rowContent.put("distributed_planning_time_ms", x));
        rowContent.put("peak_user_memory_bytes", queryStatistics.getPeakTotalNonRevocableMemoryBytes());
        rowContent.put("peak_total_non_revocable_memory_bytes", queryStatistics.getPeakTotalNonRevocableMemoryBytes());
        rowContent.put("peak_task_total_memory", queryStatistics.peakTaskTotalMemory());
        rowContent.put("total_bytes", queryStatistics.getTotalBytes());
        rowContent.put("total_rows", queryStatistics.getTotalRows());
        rowContent.put("output_bytes", queryStatistics.getOutputBytes());
        rowContent.put("output_rows", queryStatistics.getOutputRows());
        rowContent.put("written_bytes", queryStatistics.getWrittenBytes());
        rowContent.put("written_rows", queryStatistics.getWrittenRows());
        rowContent.put("cumulative_memory_bytesecond", queryStatistics.getCumulativeMemory());
        rowContent.put("stage_gc_statistics",
                queryStatistics.getStageGcStatistics().stream()
                        .map(x -> {
                            Map<String, Object> gcStat = new HashMap<>();
                            gcStat.put("stage_id", x.getStageId());
                            gcStat.put("tasks", x.getTasks());
                            gcStat.put("full_gc_tasks", x.getFullGcTasks());
                            gcStat.put("min_full_gc_sec", x.getMinFullGcSec());
                            gcStat.put("max_full_gc_sec", x.getMaxFullGcSec());
                            gcStat.put("total_full_gc_sec", x.getTotalFullGcSec());
                            gcStat.put("average_full_gc_sec", x.getAverageFullGcSec());
                            return gcStat;
                        }).collect(toList())
        );
        rowContent.put("splits", queryStatistics.getCompletedSplits());
        rowContent.put("complete", queryStatistics.isComplete());
        rowContent.put("cpu_time_distribution",
                queryStatistics.getCpuTimeDistribution().stream()
                        .map(x -> {
                            Map<String, Object> cpuDist = new HashMap<>();
                            cpuDist.put("stage_id", x.getStageId());
                            cpuDist.put("tasks", x.getTasks());
                            cpuDist.put("p25", x.getP25());
                            cpuDist.put("p50", x.getP50());
                            cpuDist.put("p75", x.getP75());
                            cpuDist.put("p90", x.getP90());
                            cpuDist.put("p95", x.getP95());
                            cpuDist.put("p99", x.getP99());
                            cpuDist.put("min_", x.getMin());
                            cpuDist.put("max_", x.getMax());
                            cpuDist.put("total_", x.getTotal());
                            cpuDist.put("average_", x.getAverage());

                            return cpuDist;
                        }).collect(toList()));
        rowContent.put("operator_summaries", queryStatistics.getOperatorSummaries());

        // QueryContext
        QueryContext queryContext = queryCompletedEvent.getContext();
        rowContent.put("user_", queryContext.getUser());
        queryContext.getPrincipal().ifPresent(x -> rowContent.put("principal", x));
        queryContext.getRemoteClientAddress().ifPresent(x -> rowContent.put("remote_client_address", x));
        queryContext.getUserAgent().ifPresent(x -> rowContent.put("user_gent", x));
        queryContext.getClientInfo().ifPresent(x -> rowContent.put("client_info", x));
        rowContent.put("client_tags", queryContext.getClientTags().stream().collect(toList()));
        rowContent.put("client_capabilities", queryContext.getClientCapabilities().stream().collect(toList()));
        queryContext.getSource().ifPresent(x -> rowContent.put("source", x));
        queryContext.getCatalog().ifPresent(x -> rowContent.put("catalog", x));
        queryContext.getSchema().ifPresent(x -> rowContent.put("schema_", x));
        queryContext.getResourceGroupId().ifPresent(x -> rowContent.put("resource_group_id_segments", x.getSegments()));

        // QueryIOMetadata

        // QueryFailureInfo

        return rowContent;
    }
}
