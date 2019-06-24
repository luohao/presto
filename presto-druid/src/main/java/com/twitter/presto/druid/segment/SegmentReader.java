package com.twitter.presto.druid.segment;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;

public interface SegmentReader
{
    int nextBatch()
            throws IOException;

    Block readBlock(Type type, String columnName)
            throws IOException;
}
