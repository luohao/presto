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

import io.airlift.http.client.jetty.JettyHttpClient;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertTrue;

public class TestDruidClient
{
    @Test
    void test()
    {
        DruidConfig testConfig = new DruidConfig()
                .setDruidCoordinatorUrl("http://localhost:8081")
                .setDruidBrokerUrl("http://localhost:8082");

        DruidClient client = new DruidClient(testConfig, new JettyHttpClient());

        List<SegmentAnalysis> metadata = client.getAllSegmentMetadata("wikipedia");

        assertTrue(metadata.size() > 0);
    }
}
