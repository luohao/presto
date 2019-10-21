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
package io.prestosql.plugin.druid.segment;

import io.prestosql.plugin.druid.DataInputSource;
import io.prestosql.plugin.druid.DataInputSourceId;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static io.prestosql.plugin.druid.DruidErrorCode.DRUID_DEEP_STORAGE_ERROR;
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
            throw new PrestoException(DRUID_DEEP_STORAGE_ERROR, format("Error reading from %s at position %s", id, position), e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }
}
