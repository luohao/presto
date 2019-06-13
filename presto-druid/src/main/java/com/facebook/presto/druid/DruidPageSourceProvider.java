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

import com.facebook.presto.druid.metadata.SegmentInfo;
import com.facebook.presto.druid.segment.DruidSegmentPageSource;
import com.facebook.presto.druid.segment.DruidSegmentReader;
import com.facebook.presto.druid.segment.HdfsIndexSource;
import com.facebook.presto.druid.segment.SegmentSourceId;
import com.facebook.presto.druid.segment.ZippedSegment;
import com.facebook.presto.druid.util.HdfsUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_DEEP_STORAGE_FILESYSTEM_ERROR;
import static com.facebook.presto.druid.metadata.SegmentInfo.DeepStorageType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DruidPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsUtil hdfsUtil;

    @Inject
    public DruidPageSourceProvider(HdfsUtil hdfsUtil)
    {
        this.hdfsUtil = requireNonNull(hdfsUtil, "hdfsUtil is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns)
    {
        DruidSplit druidSplit = (DruidSplit) split;

        // parse Druid segment locations
        SegmentInfo segmentInfo = druidSplit.getSegmentInfo();

        DeepStorageType type = segmentInfo.getDeepStorageType();
        checkArgument(type.equals(DeepStorageType.HDFS));
        Path hdfsPath = new Path(segmentInfo.getDeepStoragePath());
        try {
            FileSystem fileSystem = hdfsUtil.getFileSystem(hdfsPath);
            long fileSize = fileSystem.getFileStatus(hdfsPath).getLen();
            FSDataInputStream inputStream = hdfsUtil.getInputStream(hdfsPath);
            SegmentSourceId segmentSourceId = new SegmentSourceId(hdfsPath.toString());
            HdfsIndexSource indexSource = new HdfsIndexSource(segmentSourceId, inputStream, fileSize);

            return new DruidSegmentPageSource(
                    druidSplit.getSegmentInfo(),
                    columns,
                    new DruidSegmentReader(new ZippedSegment(segmentSourceId, indexSource), columns));
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_DEEP_STORAGE_FILESYSTEM_ERROR, format("failed to open file %s", hdfsPath), e);
        }
    }
}
