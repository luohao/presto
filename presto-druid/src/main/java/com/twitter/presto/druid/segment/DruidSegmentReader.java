package com.twitter.presto.druid.segment;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.twitter.presto.druid.DruidColumnHandle;
import com.twitter.presto.druid.column.ColumnReader;
import com.twitter.presto.druid.column.SimpleReadableOffset;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.BaseColumn;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.twitter.presto.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;
import static com.twitter.presto.druid.column.ColumnReader.createColumnReader;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class DruidSegmentReader
        implements SegmentReader
{
    private static final int BATCH_SIZE = 1024;

    private final QueryableIndex queryableIndex;
    private final Map<String, ColumnReader> columnValueSelectors = new HashMap<>();
    private final long totalRowCount;

    private long currentPosition;
    private int currentBatchSize;

    public DruidSegmentReader(SegmentIndexSource segmentIndexSource, List<ColumnHandle> columns)
    {
        try {
            this.queryableIndex = segmentIndexSource.loadIndex(columns);
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
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, "failed to load druid segment");
        }
    }

    @Override
    public int nextBatch()
            throws IOException
    {
        // TODO: dynamic batch sizing
        currentBatchSize = toIntExact(min(BATCH_SIZE, totalRowCount - currentPosition));
        currentPosition += currentBatchSize;
        return currentBatchSize;
    }

    @Override
    public Block readBlock(Type type, String columnName)
            throws IOException
    {
        return columnValueSelectors.get(columnName).readBlock(type, currentBatchSize);
    }
}
