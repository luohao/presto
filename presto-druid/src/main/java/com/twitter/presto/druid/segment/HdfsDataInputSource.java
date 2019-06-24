package com.twitter.presto.druid.segment;

import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static com.twitter.presto.druid.DruidErrorCode.DRUID_DEEP_STORAGE_FILESYSTEM_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HdfsDataInputSource
        implements DataInputSource
{
    private final DataInputSourceId id;
    private final FSDataInputStream inputStream;
    private final long size;
    private long readTimeNanos;
    private long readBytes;

    public HdfsDataInputSource(
            DataInputSourceId id,
            FSDataInputStream inputStream,
            long size)
    {
        this.id = requireNonNull(id, "id is null");
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
        this.size = requireNonNull(size, "size is null");
    }

    @Override
    public DataInputSourceId getId()
    {
        return id;
    }

    @Override
    public long getReadBytes()
    {
        return readBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getSize()
    {
        return size;
    }

    @Override
    public void readFully(long position, byte[] buffer)
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        long start = System.nanoTime();
        readInternal(position, buffer, bufferOffset, bufferLength);

        readTimeNanos += System.nanoTime() - start;
        readBytes += bufferLength;
    }

    private void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        try {
            inputStream.readFully(position, buffer, bufferOffset, bufferLength);
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_DEEP_STORAGE_FILESYSTEM_ERROR, format("Error reading from %s at position %s", id, position), e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }
}
