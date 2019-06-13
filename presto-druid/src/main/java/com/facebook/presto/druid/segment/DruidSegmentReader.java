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
import com.facebook.presto.druid.column.AbstractColumnReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.BaseColumn;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;
import static com.facebook.presto.druid.column.AbstractColumnReader.createColumnReader;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DruidSegmentReader
{
    private static final int BATCH_SIZE = 124;

    private final DruidSegment segment;
    private final QueryableIndex queryableIndex;
    private final Map<String, AbstractColumnReader> columnValueSelectors = new HashMap<>();
    private final long totalRowCount;

    private long currentPosition;
    private int currentBatchSize;

    public DruidSegmentReader(DruidSegment segment, List<ColumnHandle> columns)
    {
        this.segment = requireNonNull(segment, "segment is null");
        try {
            this.queryableIndex = segment.loadIndex(columns);
            totalRowCount = queryableIndex.getNumRows();
            for (ColumnHandle column : columns) {
                DruidColumnHandle druidColumn = (DruidColumnHandle) column;
                String columnName = druidColumn.getColumnName();
                Type type = druidColumn.getColumnType();
                BaseColumn baseColumn = queryableIndex.getColumnHolder(columnName).getColumn();
                ColumnValueSelector<?> valueSelector = baseColumn.makeColumnValueSelector(new SimpleReadableOffset());
                columnValueSelectors.put(columnName, createColumnReader(type, valueSelector));
            }
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, format("failed to load druid segment: %s", segment.getSegmentSourceId()));
        }
    }
    public int nextBatch()
            throws IOException
    {
        // TODO: dynamic batch sizing
        currentBatchSize = toIntExact(min(BATCH_SIZE, totalRowCount - currentPosition));
        currentPosition += currentBatchSize;
        return currentBatchSize;
    }

    public Block readBlock(Type type, String columnName)
            throws IOException
    {
        return columnValueSelectors.get(columnName).readBlock(type, currentBatchSize);
    }
}
