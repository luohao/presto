package com.twitter.presto.plugin.eventlistener.bq;

import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryOutputMetadata;
import com.facebook.presto.spi.eventlistener.StageCpuDistribution;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.facebook.presto.spi.session.ResourceEstimates;
import io.airlift.json.JsonCodec;

import java.util.Map;

public class MetadataUtils
{
    private static final JsonCodec<Map> MAP_JSON_CODEC = JsonCodec.jsonCodec(Map.class);
    private static final JsonCodec<ResourceEstimates> RESOURCE_ESTIMATES_JSON_CODEC = JsonCodec.jsonCodec(ResourceEstimates.class);
    private static final JsonCodec<QueryInputMetadata> QUERY_INPUT_METADATA_JSON_CODEC = JsonCodec.jsonCodec(QueryInputMetadata.class);
    private static final JsonCodec<QueryOutputMetadata> QUERY_OUTPUT_METADATA_JSON_CODEC = JsonCodec.jsonCodec(QueryOutputMetadata.class);
    private static final JsonCodec<StageGcStatistics> STAGE_GC_STATISTICS_JSON_CODEC = JsonCodec.jsonCodec(StageGcStatistics.class);
    private static final JsonCodec<StageCpuDistribution> STAGE_CPU_DISTRIBUTION_JSON_CODEC = JsonCodec.jsonCodec(StageCpuDistribution.class);

    public static String mapToJson(Map map)
    {
        return toJson(map, MAP_JSON_CODEC);
    }

    public static String resourceEstimatesToJson(ResourceEstimates resourceEstimates)
    {
        return toJson(resourceEstimates, RESOURCE_ESTIMATES_JSON_CODEC);
    }

    public static String queryInputMetadataToJson(QueryInputMetadata inputMetadata)
    {
        return toJson(inputMetadata, QUERY_INPUT_METADATA_JSON_CODEC);
    }

    public static String queryOutputMetadataToJson(QueryOutputMetadata outputMetadata)
    {
        return toJson(outputMetadata, QUERY_OUTPUT_METADATA_JSON_CODEC);
    }

    public static String stageGcStatisticsToJson(StageGcStatistics stageGcStatistics)
    {
        return toJson(stageGcStatistics, STAGE_GC_STATISTICS_JSON_CODEC);
    }

    public static String stageCpuDistributionToJson(StageCpuDistribution stageCpuDistribution)
    {
        return toJson(stageCpuDistribution, STAGE_CPU_DISTRIBUTION_JSON_CODEC);
    }

    private static <T> String toJson(T object, JsonCodec<T> codec)
    {
        return codec.toJson(object);
    }
}
