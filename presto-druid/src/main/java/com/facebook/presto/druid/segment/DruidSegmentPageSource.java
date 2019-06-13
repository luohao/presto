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

import com.facebook.presto.druid.DruidColumnHandle;
import com.facebook.presto.druid.metadata.SegmentInfo;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_SEGMENT_READ_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DruidSegmentPageSource
        implements ConnectorPageSource
{
    private final SegmentInfo segmentInfo;
    private final List<ColumnHandle> columns;
    private final DruidSegmentReader segmentReader;

    private int batchId;
    private boolean closed;

    public DruidSegmentPageSource(
            SegmentInfo segmentInfo,
            List<ColumnHandle> columns,
            DruidSegmentReader segmentReader)
    {
        this.segmentInfo = requireNonNull(segmentInfo, "segment info is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.segmentReader = requireNonNull(segmentReader, "segmentReader is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        try {
            batchId++;
            int batchSize = segmentReader.nextBatch();
            if (batchSize <= 0) {
                close();
                return null;
            }
            Block[] blocks = new Block[columns.size()];
            for (int i = 0; i < blocks.length; ++i) {
                DruidColumnHandle columnHandle = (DruidColumnHandle) columns.get(i);
//                blocks[i] = segmentReader.readBlock(columnHandle.getColumnType(), columnHandle.getColumnName());
                blocks[i] = new LazyBlock(batchSize, new SegmentBlockLoader(columnHandle.getColumnType(), columnHandle.getColumnName()));
            }
            return new Page(batchSize, blocks);
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_SEGMENT_READ_ERROR, e);
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        closed = true;
        // TODO: close all column reader and value selectors
    }

    private final class SegmentBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final Type type;
        private final String name;
        private boolean loaded;

        public SegmentBlockLoader(Type type, String name)
        {
            this.type = requireNonNull(type, "type is null");
            this.name = requireNonNull(name, "name is null");
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            checkState(batchId == expectedBatchId);

            try {
                Block block = segmentReader.readBlock(type, name);
                lazyBlock.setBlock(block);
            }
            catch (IOException e) {
                throw new PrestoException(DRUID_SEGMENT_READ_ERROR, e);
            }

            loaded = true;
        }
    }
}
