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

import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class DruidQueryRunnerBuilder
        extends DistributedQueryRunner.Builder
{
    private static final Session DEFAULT_SESSION = testSessionBuilder()
            .setSource("test")
            .setCatalog("druid")
            .setSchema("druid")
            .build();

    private DruidQueryRunnerBuilder()
    {
        super(DEFAULT_SESSION);
    }

    public static DruidQueryRunnerBuilder builder()
    {
        return new DruidQueryRunnerBuilder();
    }

    @Override
    public DistributedQueryRunner build()
            throws Exception
    {
        DistributedQueryRunner queryRunner = super.build();
        try {
            queryRunner.installPlugin(new DruidPlugin());
            Map<String, String> properties = ImmutableMap.of(
                    "druid-coordinator-url", "http://localhost:8081",
                    "druid-broker-url", "http://localhost:8082");
            queryRunner.createCatalog("druid", "druid", properties);

            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
