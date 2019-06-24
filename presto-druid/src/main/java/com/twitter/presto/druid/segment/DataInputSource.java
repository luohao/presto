package com.twitter.presto.druid.segment;

import java.io.Closeable;
import java.io.IOException;

public interface DataInputSource
        extends Closeable
{
    DataInputSourceId getId();

    long getReadBytes();

    long getReadTimeNanos();

    long getSize();

    void readFully(long position, byte[] buffer)
            throws IOException;

    void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException;

    @Override
    default void close()
            throws IOException
    {
    }
}
