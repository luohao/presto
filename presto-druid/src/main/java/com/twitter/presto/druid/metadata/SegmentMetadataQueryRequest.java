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
package com.twitter.presto.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SegmentMetadataQueryRequest
{
    @JsonProperty("queryType")
    private static final String QUERY_TYPE = "segmentMetadata";

    private final String dataSource;
    private final Optional<List<String>> intervals;

    @JsonCreator
    public SegmentMetadataQueryRequest(
            @JsonProperty("dataSource") String dataSource,
            @JsonProperty("intervals") Optional<List<String>> intervals)
    {
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.intervals = requireNonNull(intervals, "intervals is null");
    }

    @JsonProperty
    public String getQueryType()
    {
        return QUERY_TYPE;
    }

    @JsonProperty
    public String getDataSource()
    {
        return dataSource;
    }

    @JsonProperty
    public Optional<List<String>> getIntervals()
    {
        return intervals;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryType", QUERY_TYPE)
                .add("dataSource", dataSource)
                .add("intervals", intervals)
                .toString();
    }

    public static class Builder
    {
        private String dataSource;
        private ImmutableList.Builder<Interval> intervalBuilder = ImmutableList.builder();

        public Builder dataSource(String dataSource)
        {
            this.dataSource = dataSource;
            return this;
        }

        public Builder withInterval(long startInstant, long endInstant)
        {
            this.intervalBuilder.add(new Interval(startInstant, endInstant, ISOChronology.getInstanceUTC()));
            return this;
        }

        public Builder withInterval(String interval)
        {
            this.intervalBuilder.add(new Interval(interval, ISOChronology.getInstanceUTC()));
            return this;
        }

        public Builder withInterval(String format, Object... formatArgs)
        {
            return withInterval(StringUtils.format(format, formatArgs));
        }

        public Builder withEternity()
        {
            return withInterval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT);
        }

        public SegmentMetadataQueryRequest build()
        {
            List<String> intervals = intervalBuilder.build().stream()
                    .map(Interval::toString)
                    .collect(toImmutableList());
            return new SegmentMetadataQueryRequest(dataSource, intervals.size() == 0 ? Optional.empty() : Optional.of(intervals));
        }
    }
}
