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

package com.twitter.presto.druid;

import com.twitter.presto.druid.metadata.SegmentInfo;
import com.twitter.presto.druid.metadata.SegmentMetadataQueryRequest;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;

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
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DruidClient
{
    // Druid API endpoints
    private static final String METADATA_PATH = "/druid/coordinator/v1/metadata";
    private static final String QUERY_PATH = "/druid/v2";

    // codec
    private static final JsonCodec<List<String>> LIST_STRING_CODEC = listJsonCodec(String.class);
    private static final JsonCodec<List<SegmentAnalysis>> LIST_SEGMENT_ANALYSIS_CODEC = listJsonCodec(SegmentAnalysis.class);
    private static final JsonCodec<SegmentMetadataQueryRequest> SEGMENT_METADATA_REQUEST_CODEC = jsonCodec(SegmentMetadataQueryRequest.class);
    private static final JsonCodec<SegmentInfo> SEGMENT_INFO_JSON_CODEC = jsonCodec(SegmentInfo.class);
    private static final JsonCodec<List<SegmentInfo>> LIST_SEGMENT_INFO_JSON_CODEC = listJsonCodec(SegmentInfo.class);

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

    public URI getDruidCoordinator()
    {
        return druidCoordinator;
    }

    public URI getDruidBroker()
    {
        return druidBroker;
    }

    public List<String> getDataSources(boolean includeDisabled)
    {
        HttpUriBuilder uriBuilder = uriBuilderFrom(druidCoordinator).replacePath(METADATA_PATH).appendPath("datasources");

        if (includeDisabled) {
            uriBuilder.addParameter("includeDisabled");
        }

        // TODO: maintain a local cache
        return httpClient.execute(
                prepareGet().setUri(uriBuilder.build()).build(),
                createJsonResponseHandler(LIST_STRING_CODEC));
    }

    public List<SegmentAnalysis> getAllSegmentMetadata(String dataSource)
    {
        return getSegmentMetadataSince(dataSource, JodaUtils.MIN_INSTANT);
    }

    public List<SegmentAnalysis> getSegmentMetadataSince(String dataSource, long startInstant)
    {
        return getSegmentMetadataBetween(dataSource, startInstant, JodaUtils.MAX_INSTANT);
    }

    public List<SegmentAnalysis> getSegmentMetadataBetween(String dataSource, long startInstant, long endInstant)
    {
        SegmentMetadataQueryRequest.Builder builder = new SegmentMetadataQueryRequest.Builder().dataSource(dataSource).withInterval(startInstant, endInstant);
        byte[] requestBody = SEGMENT_METADATA_REQUEST_CODEC.toJsonBytes(builder.build());

        URI uri = uriBuilderFrom(druidBroker).replacePath(QUERY_PATH).build();
        Request request = setContentTypeHeaders(preparePost())
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(requestBody))
                .build();

        return httpClient.execute(request, createJsonResponseHandler(LIST_SEGMENT_ANALYSIS_CODEC));
    }

    public List<String> getDataSegmentIds(String dataSource)
    {
        URI uri = uriBuilderFrom(druidCoordinator)
                .replacePath(METADATA_PATH)
                .appendPath(format("datasources/%s/segments", dataSource))
                .build();
        Request request = setContentTypeHeaders(prepareGet())
                .setUri(uri)
                .build();

        return httpClient.execute(request, createJsonResponseHandler(LIST_STRING_CODEC));
    }

    public SegmentInfo getSingleSegmentInfo(String dataSource, String segmentId)
    {
        URI uri = uriBuilderFrom(druidCoordinator)
                .replacePath(METADATA_PATH)
                .appendPath(format("datasources/%s/segments/%s", dataSource, segmentId))
                .build();
        Request request = setContentTypeHeaders(prepareGet())
                .setUri(uri)
                .build();

        return httpClient.execute(request, createJsonResponseHandler(SEGMENT_INFO_JSON_CODEC));
    }

    public List<SegmentInfo> getAllSegmentInfos(String dataSource)
    {
        URI uri = uriBuilderFrom(druidCoordinator)
                .replacePath(METADATA_PATH)
                .appendPath(format("datasources/%s/segments", dataSource))
                .addParameter("full")
                .build();
        Request request = setContentTypeHeaders(prepareGet())
                .setUri(uri)
                .build();

        return httpClient.execute(request, createJsonResponseHandler(LIST_SEGMENT_INFO_JSON_CODEC));
    }

    private static Request.Builder setContentTypeHeaders(Request.Builder requestBuilder)
    {
        return requestBuilder
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(ACCEPT, JSON_UTF_8.toString());
    }
}
