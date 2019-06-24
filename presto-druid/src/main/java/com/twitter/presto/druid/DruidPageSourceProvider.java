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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.twitter.presto.druid.metadata.SegmentInfo;
import com.twitter.presto.druid.segment.DataInputSourceId;
import com.twitter.presto.druid.segment.DruidSegmentReader;
import com.twitter.presto.druid.segment.HdfsDataInputSource;
import com.twitter.presto.druid.segment.IndexFileSource;
import com.twitter.presto.druid.segment.SegmentColumnSource;
import com.twitter.presto.druid.segment.SegmentIndexSource;
import com.twitter.presto.druid.segment.SmooshedColumnSource;
import com.twitter.presto.druid.segment.V9SegmentIndexSource;
import com.twitter.presto.druid.segment.ZipIndexFileSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.twitter.presto.druid.DruidErrorCode.DRUID_DEEP_STORAGE_FILESYSTEM_ERROR;
import static com.twitter.presto.druid.metadata.SegmentInfo.DeepStorageType;

public class DruidPageSourceProvider
        implements ConnectorPageSourceProvider
{
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
        try {
            Path hdfsPath = new Path(segmentInfo.getDeepStoragePath());
            FileSystem fileSystem = hdfsPath.getFileSystem(new Configuration());
            long fileSize = fileSystem.getFileStatus(hdfsPath).getLen();
            FSDataInputStream inputStream = fileSystem.open(hdfsPath);
            DataInputSourceId dataInputSourceId = new DataInputSourceId(hdfsPath.toString());
            HdfsDataInputSource dataInputSource = new HdfsDataInputSource(dataInputSourceId, inputStream, fileSize);
            IndexFileSource indexFileSource = new ZipIndexFileSource(dataInputSource);
            SegmentColumnSource segmentColumnSource = new SmooshedColumnSource(indexFileSource);
            SegmentIndexSource segmentIndexSource = new V9SegmentIndexSource(segmentColumnSource);

            return new DruidSegmentPageSource(
                    druidSplit.getSegmentInfo(),
                    columns,
                    new DruidSegmentReader(segmentIndexSource, columns));
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_DEEP_STORAGE_FILESYSTEM_ERROR, e);
        }
    }
}
