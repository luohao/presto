package com.twitter.presto.plugin.eventlistener.bq;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
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
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.twitter.presto.plugin.eventlistener.bq.MetadataUtils.mapToJson;
import static com.twitter.presto.plugin.eventlistener.bq.MetadataUtils.queryOutputMetadataToJson;
import static com.twitter.presto.plugin.eventlistener.bq.MetadataUtils.resourceEstimatesToJson;
import static java.util.stream.Collectors.toList;

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
        rowContent.put("stage_gc_statistics", queryStatistics.getStageGcStatistics().stream().map(MetadataUtils::stageGcStatisticsToJson).collect(toList()));
        rowContent.put("splits", queryStatistics.getCompletedSplits());
        rowContent.put("complete", queryStatistics.isComplete());
        rowContent.put("cpu_time_distribution", queryStatistics.getCpuTimeDistribution().stream().map(MetadataUtils::stageCpuDistributionToJson).collect(toList()));
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
        rowContent.put("session_properties_json", mapToJson(queryContext.getSessionProperties()));
        rowContent.put("resource_estimates_json", resourceEstimatesToJson(queryContext.getResourceEstimates()));
        rowContent.put("server_address", queryContext.getServerAddress());
        rowContent.put("server_version", queryContext.getServerVersion());
        rowContent.put("environment", queryContext.getEnvironment());

        // QueryIOMetadata
        QueryIOMetadata queryIOMetadata = queryCompletedEvent.getIoMetadata();
        rowContent.put("io_inputs_metadata_json", queryIOMetadata.getInputs().stream().map(MetadataUtils::queryInputMetadataToJson).collect(Collectors.toList()));
        queryIOMetadata.getOutput().ifPresent(x -> rowContent.put("io_output_metadata_json", queryOutputMetadataToJson(x)));

        // QueryFailureInfo
        queryCompletedEvent.getFailureInfo().ifPresent(
                failureInfo -> {
                    rowContent.put("error_code_id", failureInfo.getErrorCode().getCode());
                    rowContent.put("error_code_name", failureInfo.getErrorCode().getName());
                    rowContent.put("error_code_type", failureInfo.getErrorCode().getType());
                    failureInfo.getFailureType().ifPresent(x -> rowContent.put("failure_type", x));
                    failureInfo.getFailureMessage().ifPresent(x -> rowContent.put("failure_message", x));
                    failureInfo.getFailureTask().ifPresent(x -> rowContent.put("failure_task", x));
                    failureInfo.getFailureHost().ifPresent(x -> rowContent.put("failure_host", x));
                    rowContent.put("failures_json", failureInfo.getFailuresJson());
                });

        return rowContent;
    }
}
