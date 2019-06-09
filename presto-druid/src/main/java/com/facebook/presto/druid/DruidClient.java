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

package com.facebook.presto.druid;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.util.Objects.requireNonNull;

public class DruidClient
{
    // Druid endpoints
    private static final String METADATA_PATH = "/druid/coordinator/v1/metadata";

    // codec
    private static final JsonCodec<List<String>> LIST_STRING_CODEC = listJsonCodec(String.class);

    private final HttpClient httpClient;
    private final URI druidCoordinator;

    @Inject
    public DruidClient(DruidConfig config, @ForDruidClient HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.druidCoordinator = URI.create(config.getDruidCoordinatorUrl());
    }

    public List<String> getDataSources(boolean includeDisabled)
    {
        HttpUriBuilder uriBuilder =
                uriBuilderFrom(druidCoordinator)
                        .replacePath(METADATA_PATH)
                        .appendPath("datasources");

        if (includeDisabled) {
            uriBuilder.addParameter("includeDisabled");
        }

        // TODO: should we do it asynchronously?
        return httpClient.execute(
                prepareGet().setUri(uriBuilder.build()).build(),
                createJsonResponseHandler(LIST_STRING_CODEC));
    }
}
