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
package com.facebook.presto.druid.segment;

import com.facebook.presto.druid.metadata.SegmentInfo;
import org.apache.druid.timeline.SegmentId;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SegmentSourceId
{
    private final String id;

    public SegmentSourceId(String id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SegmentSourceId that = (SegmentSourceId) o;
        return Objects.equals(id, that.id);
    }

    public static SegmentSourceId of(SegmentInfo segmentInfo)
    {
        Optional<Map<String, String>> shardSpec = segmentInfo.getShardSpec();
        int partitionNumber = 0;
        if (shardSpec.isPresent()) {
            partitionNumber = Integer.valueOf(shardSpec.get().get("partitionNum"));
        }
        SegmentId segmentId = SegmentId.of(segmentInfo.getDataSource(), segmentInfo.getInterval(), segmentInfo.getVersion(), partitionNumber);
        return new SegmentSourceId(segmentId.toString());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public String toString()
    {
        return id;
    }
}
