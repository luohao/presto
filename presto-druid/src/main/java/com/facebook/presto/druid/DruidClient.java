package com.facebook.presto.druid;

import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.util.Objects.requireNonNull;

public class DruidClient
{
    private static final String DRUID_SCHEMA = "druid";

    // druid endpoints
    private static final String METADATA_PATH = "/druid/coordinator/v1/metadata";

    // codec
    private static final JsonCodec<List<String>> LIST_STRING_CODEC = listJsonCodec(String.class);

    private final HttpClient httpClient;
    private final URI druidCoordinator;

    @Inject
    public DruidClient(DruidConfig config, HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.druidCoordinator = URI.create(config.getDruidCoordinatorUrl());
    }

    public Set<String> getSchemaNames()
    {
        // According to Druid SQL specification, all datasources will be in druid schema
        return ImmutableSet.of(DRUID_SCHEMA);
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        checkArgument(schema.equals(DRUID_SCHEMA));
        return getDataSources(true);
    }

    private Set<String> getDataSources(boolean includeDisabled)
    {
        HttpUriBuilder uriBuilder = uriBuilderFrom(druidCoordinator)
                .replacePath(METADATA_PATH)
                .appendPath("datasources");

        if (includeDisabled) {
            uriBuilder.addParameter("includeDisabled");
        }

        // TODO: should we do it asynchronously?
        List<String> response = httpClient.execute(
                prepareGet().setUri(uriBuilder.build()).build(),
                createJsonResponseHandler(LIST_STRING_CODEC));
        return ImmutableSet.copyOf(response);
    }
}
