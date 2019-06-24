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
import io.airlift.http.client.jetty.JettyHttpClient;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestDruidClient
{
    private final DruidClient client;

    public TestDruidClient()
    {
        DruidConfig testConfig = new DruidConfig()
                .setDruidCoordinatorUrl("http://localhost:8081")
                .setDruidBrokerUrl("http://localhost:8082");

        client = new DruidClient(testConfig, new JettyHttpClient());
    }

    @Test
    void testSegments()
    {
        String dataSource = "wikipedia";
        List<String> ids = client.getDataSegmentIds(dataSource);
        assertEquals(ids.size(), 1);
        SegmentInfo segmentInfo = client.getSingleSegmentInfo(dataSource, ids.get(0));
        List<SegmentInfo> segmentInfos = client.getAllSegmentInfos(dataSource);
        assertEquals(segmentInfos.size(), 1);
        assertEquals(segmentInfos.get(0), segmentInfo);
    }
}
