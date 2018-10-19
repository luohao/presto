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
package com.twitter.presto.maintenance;

import com.facebook.presto.server.remotetask.Backoff;
import com.facebook.presto.spi.NodeState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.json.JSONObject;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;

@Path("/")
public class MaintenanceCoordinatorResource
{
    private static final Logger log = Logger.get(MaintenanceCoordinatorResource.class);
    private final HttpClient httpClient;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final String version;

    @Inject
    public MaintenanceCoordinatorResource(@ForAurora HttpClient httpClient, MaintenanceCoordinatorConfig config)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.executor = newCachedThreadPool(daemonThreadsNamed("proxy-%s"));
        this.scheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("maintenance-coordinator-%s"));
        this.version = config.getVersion();
    }

    @POST
    @Path("/")
    @Consumes(APPLICATION_JSON)
    public void canDrain(
            String jsonString,
            @Suspended AsyncResponse asyncResponse)
    {
        URI nodeUri = extractHostUri(jsonString);
        URI stateInfoUri = uriBuilderFrom(nodeUri).appendPath("/v1/info/state").build();

        // synchronously send SHUTTING_DOWN request to worker node
        Request request = preparePut()
                .setUri(stateInfoUri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(jsonCodec(NodeState.class), NodeState.SHUTTING_DOWN))
                .build();

        StatusResponseHandler.StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        if (response.getStatusCode() != OK.code()) {
            // FIXME: can't shutdown this node for some unknown reason. Although this should not happen from the first place, it's safe to just return false in the drain message
            throw badRequest(Response.Status.SERVICE_UNAVAILABLE, "Can't shutdown remote worker node : " + nodeUri);
        }

        // polling the status of the worker node until it's unreachable
        // this will asynchronously poll the status of the node status until the node is shut down.
        waitForShutdown(stateInfoUri, createShutdownBackoff(), asyncResponse);
    }

    // extract the worker node URI from the request body
    private URI extractHostUri(String message)
    {
        // FIXME: this part of code simply assumes the request body has a schema as demonstrated in the web page
        // See http://aurora.apache.org/documentation/latest/features/sla-requirements/#coordinator-based for more info
        JSONObject jsonBody = new JSONObject(message);
        String hostName = jsonBody
                .getJSONObject("taskConfig")
                .getJSONObject("assignedTask")
                .get("slaveHost")
                .toString();
        int port = (Integer) jsonBody
                .getJSONObject("taskConfig")
                .getJSONObject("assignedPorts")
                .get("http");

        return URI.create("http://" + hostName + ":" + port);
    }

    // wait for the node to shutdown
    // FIXME: at this moment I am polling the status of worker node from the remote node. It would be better if we can query Aurora scheduler for the status of the node which is
    // a more definite state.
    private synchronized void waitForShutdown(URI stateInfoUri, Backoff shutdownBackoff, AsyncResponse asyncResponse)
    {
        Request request = prepareGet()
                .setUri(stateInfoUri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();

        ListenableFuture<JsonResponse<NodeState>> responseFuture =
                FluentFuture
                        .from(httpClient.executeAsync(request, createFullJsonResponseHandler(jsonCodec(NodeState.class))));

        Futures.addCallback(responseFuture, new FutureCallback<JsonResponse<NodeState>>()
        {
            @Override
            public void onSuccess(JsonResponse<NodeState> result)
            {
                // when maintenance coordinator successfully receive a node state query, it implicitly indicates that the node is still up.
                if (result != null) {
                    if (result.getStatusCode() != OK.code()) {
                        log.warn("Error fetching node state from %s returned status %d: %s", stateInfoUri, result.getStatusCode(), result.getStatusMessage());
                    }
                    if (result.hasValue()
                            && result.getValue() != NodeState.SHUTTING_DOWN) {
                        badRequest(Response.Status.EXPECTATION_FAILED, "Failed to instruct remote node " + stateInfoUri + " to shutdown");
                    }
                }

                // everything checks, reschedule the next poll
                long delayNanos = shutdownBackoff.getBackoffDelayNanos();
                if (delayNanos == 0) {
                    waitForShutdown(stateInfoUri, shutdownBackoff, asyncResponse);
                }
                else {
                    scheduledExecutor.schedule(() -> waitForShutdown(stateInfoUri, shutdownBackoff, asyncResponse), delayNanos, NANOSECONDS);
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                // We treat a failure to query the node state as the signal to node down
                if (t instanceof RejectedExecutionException && httpClient.isClosed()) {
                    log.error(t, "Unable to poll state of remote node %s. HTTP client is closed.", request.getUri());
                    asyncResponse.resume(Response.ok().entity(new DrainResponse(false)).build());
                }

                // we consider the node completely shutdown, now send response to the Aurora COp.
                asyncResponse.resume(Response.ok().entity(new DrainResponse(true)).build());
            }
        }, executor);
    }

    private static Backoff createShutdownBackoff()
    {
        return new Backoff(10, new Duration(10, TimeUnit.MINUTES), Ticker.systemTicker(), ImmutableList.<Duration>builder()
                .add(new Duration(0, MILLISECONDS))
                .add(new Duration(100, MILLISECONDS))
                .add(new Duration(500, MILLISECONDS))
                .add(new Duration(1, SECONDS))
                .add(new Duration(10, SECONDS))
                .build());
    }

    private static WebApplicationException badRequest(Response.Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    public static class DrainResponse
    {
        private final boolean drain;

        @JsonCreator
        public DrainResponse(@JsonProperty("drain") boolean drain)
        {
            this.drain = drain;
        }

        @JsonProperty
        public boolean getDrain()
        {
            return drain;
        }
    }
}
