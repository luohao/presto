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

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DruidTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final SchemaTableName schemaTableName;
    private final List<String> segmentIds;

    @JsonCreator
    public DruidTableLayoutHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "table is null");
        this.segmentIds = null;
    }

    public DruidTableLayoutHandle(
            SchemaTableName schemaTableName,
            List<String> segmentIds)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "table is null");
        this.segmentIds = requireNonNull(segmentIds, "segmentIds is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonIgnore
    public Optional<List<String>> getSegmentIds()
    {
        return Optional.ofNullable(segmentIds);
    }
}
