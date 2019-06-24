package com.twitter.presto.druid.segment;

public interface SegmentColumnSource
{
    int getVersion();

    byte[] getColumnData(String name);
}
