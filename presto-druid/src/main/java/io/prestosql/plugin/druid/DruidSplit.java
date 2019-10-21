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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.druid.metadata.DruidSegmentInfo;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DruidSplit
        implements ConnectorSplit
{
    private final DruidSegmentInfo segmentInfo;
    private final HostAddress address;

    @JsonCreator
    public DruidSplit(
            @JsonProperty("segmentInfo") DruidSegmentInfo segmentInfo,
            @JsonProperty("address") HostAddress address)
    {
        this.segmentInfo = requireNonNull(segmentInfo, "segment info is null");
        this.address = requireNonNull(address, "address info is null");
    }

    @JsonProperty
    public DruidSegmentInfo getSegmentInfo()
    {
        return segmentInfo;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
