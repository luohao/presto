package com.twitter.presto.druid.segment;

import com.facebook.presto.spi.ColumnHandle;
import org.apache.druid.segment.QueryableIndex;

import java.io.IOException;
import java.util.List;

public interface SegmentIndexSource
{
    QueryableIndex loadIndex(List<ColumnHandle> columnHandles)
            throws IOException;
}
