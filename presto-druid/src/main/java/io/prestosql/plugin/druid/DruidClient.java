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
package io.prestosql.plugin.druid;

import com.google.common.base.Joiner;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.prestosql.plugin.druid.metadata.DruidColumnInfo;
import io.prestosql.plugin.druid.metadata.DruidSegmentIdWrapper;
import io.prestosql.plugin.druid.metadata.DruidSegmentInfo;
import io.prestosql.plugin.druid.metadata.DruidTableInfo;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import org.joda.time.DateTime;
import org.joda.time.Instant;

import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DruidClient
{
    private static final String ALWAYS_TRUE = "1=1";
    private static final String ALWAYS_FALSE = "1=0";

    // Druid coordinator API endpoints
    private static final String METADATA_PATH = "/druid/coordinator/v1/metadata";
    // Druid broker API endpoints
    private static final String SQL_ENDPOINT = "/druid/v2/sql";

    private static final String LIST_TABLE_QUERY = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid'";
    private static final String GET_COLUMN_TEMPLATE = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '%s'";
    private static final String GET_SEGMENTS_ID_TEMPLATE = "SELECT segment_id FROM sys.segments WHERE datasource = '%s' AND is_published = 1 AND %s";

    // codec
    private static final JsonCodec<List<DruidSegmentIdWrapper>> LIST_SEGMENT_ID_CODEC = listJsonCodec(DruidSegmentIdWrapper.class);
    private static final JsonCodec<List<DruidColumnInfo>> LIST_COLUMN_INFO_CODEC = listJsonCodec(DruidColumnInfo.class);
    private static final JsonCodec<List<DruidTableInfo>> LIST_TABLE_NAME_CODEC = listJsonCodec(DruidTableInfo.class);
    private static final JsonCodec<DruidSegmentInfo> SEGMENT_INFO_CODEC = jsonCodec(DruidSegmentInfo.class);

    private final HttpClient httpClient;
    private final URI druidCoordinator;
    private final URI druidBroker;

    @Inject
    public DruidClient(DruidConfig config, @ForDruidClient HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.druidCoordinator = URI.create(config.getDruidCoordinatorUrl());
        this.druidBroker = URI.create(config.getDruidBrokerUrl());
    }

    public URI getDruidBroker()
    {
        return druidBroker;
    }

    public List<String> getTables()
    {
        return httpClient.execute(prepareQuery(LIST_TABLE_QUERY), createJsonResponseHandler(LIST_TABLE_NAME_CODEC)).stream()
                .map(DruidTableInfo::getTableName)
                .collect(toImmutableList());
    }

    public List<DruidColumnInfo> getColumnDataType(String tableName)
    {
        return httpClient.execute(prepareQuery(format(GET_COLUMN_TEMPLATE, tableName)), createJsonResponseHandler(LIST_COLUMN_INFO_CODEC));
    }

    public List<String> getAllDataSegmentId(String tableName)
    {
        return getDataSegmentIdInDomain(tableName, Domain.all(TIMESTAMP));
    }

    public List<String> getDataSegmentIdInDomain(String tableName, Domain domain)
    {
        return httpClient.execute(prepareQuery(format(GET_SEGMENTS_ID_TEMPLATE, tableName, toPredicate(domain))), createJsonResponseHandler(LIST_SEGMENT_ID_CODEC)).stream()
                .map(wrapper -> wrapper.getSegmentId())
                .collect(toImmutableList());
    }

    public DruidSegmentInfo getSingleSegmentInfo(String dataSource, String segmentId)
    {
        URI uri = uriBuilderFrom(druidCoordinator)
                .replacePath(METADATA_PATH)
                .appendPath(format("datasources/%s/segments/%s", dataSource, segmentId))
                .build();
        Request request = setContentTypeHeaders(prepareGet())
                .setUri(uri)
                .build();

        return httpClient.execute(request, createJsonResponseHandler(SEGMENT_INFO_CODEC));
    }

    private static Request.Builder setContentTypeHeaders(Request.Builder requestBuilder)
    {
        return requestBuilder
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(ACCEPT, JSON_UTF_8.toString());
    }

    private static byte[] createRequestBody(String query)
    {
        return format("{\"query\":\"%s\"}\n", query).getBytes();
    }

    private Request prepareQuery(String query)
    {
        HttpUriBuilder uriBuilder = uriBuilderFrom(druidBroker).replacePath(SQL_ENDPOINT);

        return setContentTypeHeaders(preparePost())
                .setUri(uriBuilder.build())
                .setBodyGenerator(createStaticBodyGenerator(createRequestBody(query)))
                .build();
    }

    private String toPredicate(Domain domain)
    {
        checkState(domain.getType() == TIMESTAMP);
        if (domain.getValues().isNone()) {
            return ALWAYS_FALSE;
        }

        if (domain.getValues().isAll()) {
            return ALWAYS_TRUE;
        }

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(format("\\\"end\\\" > '%s'", formatTimestamp(range.getLow().getValue())));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(format("\\\"end\\\" >= '%s'", formatTimestamp(range.getLow().getValue())));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        case EXACTLY:
                            rangeConjuncts.add(format("\\\"start\\\" <= '%s'", formatTimestamp(range.getHigh().getValue())));
                            break;
                        case BELOW:
                            rangeConjuncts.add(format("\\\"start\\\" < '%s'", formatTimestamp(range.getHigh().getValue())));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values as an equality
        singleValues.stream()
                .map(value -> format("( \\\"start\\\" <= '%s' AND \\\"end\\\" >= '%s' )", formatTimestamp(value), formatTimestamp(value)))
                .forEach(disjunct -> disjuncts.add(disjunct));

        checkState(!disjuncts.isEmpty());
        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String formatTimestamp(Object object)
    {
        Instant instant = new Instant((long) object);
        return instant.toString();
    }
}
