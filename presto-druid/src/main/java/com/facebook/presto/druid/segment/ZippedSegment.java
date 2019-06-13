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
import com.facebook.presto.druid.zip.ZipFileEntry;
import com.facebook.presto.druid.zip.ZipReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.SimpleQueryableIndex;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.BitmapSerde;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.joda.time.Interval;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_ZIP_INDEX_FILE_ERROR;
import static com.facebook.presto.druid.util.FileSmoosher.makeChunkFileName;
import static com.facebook.presto.druid.util.FileSmoosher.makeMetaFileName;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.druid.segment.IndexIO.V9_VERSION;
import static org.apache.druid.segment.column.ColumnHolder.TIME_COLUMN_NAME;
import static org.apache.druid.segment.data.GenericIndexed.STRING_STRATEGY;

public class ZippedSegment
        implements DruidSegment
{
    private static final Logger log = Logger.get(ZippedSegment.class);

    private static final String SMOOSH_METADATA_FILE_NAME = makeMetaFileName();
    private static final String VERSION_FILE_NAME = "version.bin";
    private static final String INDEX_METADATA_FILE_NAME = "index.drd";
    private static final String SEGMENT_METADATA_FILE_NAME = "metadata.drd";

    private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
    private static final SerializerUtils SERIALIZER_UTILS = new SerializerUtils();

    private final SegmentSourceId segmentSourceId;
    //    private final IndexSource indexSource;
    private final ZipReader zipReader;
    private final List<String> outFiles;
    private final Map<String, SmooshFileMetadata> internalFiles = new TreeMap<>();

    public ZippedSegment(SegmentSourceId segmentSourceId, IndexSource indexSource)
    {
        this.segmentSourceId = requireNonNull(segmentSourceId, "segment srouce id is null");
//        this.indexSource = requireNonNull(indexSource, "index source is null");
        this.zipReader = new ZipReader(indexSource);
        this.outFiles = new ArrayList<>();
        loadSmooshFileMetadata();
    }

    public byte[] getColumnData(String name)
    {
        SmooshFileMetadata metadata = internalFiles.get(name);
        if (metadata == null) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Internal file %s doesn't exist", name));
        }
        String fileName = makeChunkFileName(metadata.getFileNum());
        int fileStart = metadata.getStartOffset();
        int fileSize = metadata.getEndOffset() - fileStart;

        byte[] buffer = new byte[fileSize];
        readOutFile(fileName, fileStart, buffer);
        return buffer;
    }

    private void loadSmooshFileMetadata()
    {
        ZipFileEntry metadataEntry = zipReader.getFileEntry(SMOOSH_METADATA_FILE_NAME);

        if (metadataEntry == null) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Malformed index.zip: no %s in the archive.", SMOOSH_METADATA_FILE_NAME));
        }

        byte[] metadata = new byte[(int) metadataEntry.getSize()];
        zipReader.readFully(metadataEntry, 0, metadata, 0, metadata.length);
        try (BufferedReader in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(metadata)))) {
            String line = in.readLine();
            if (line == null) {
                throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Malformed metadata file: first line should be version,maxChunkSize,numChunks, got null."));
            }

            String[] splits = line.split(",");
            if (!"v1".equals(splits[0])) {
                throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Malformed metadata file: unknown version[%s], v1 is all I know.", splits[0]));
            }
            if (splits.length != 3) {
                throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Malformed metadata file: wrong number of splits[%d] in line[%s]", splits.length,
                        line));
            }
            final Integer numFiles = Integer.valueOf(splits[2]);
            for (int i = 0; i < numFiles; ++i) {
                outFiles.add(makeChunkFileName(i));
            }

            while ((line = in.readLine()) != null) {
                splits = line.split(",");

                if (splits.length != 4) {
                    throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Malformed metadata file: wrong number of splits[%d] in line[%s]", splits.length, line));
                }
                internalFiles.put(splits[0].toLowerCase(ENGLISH), new SmooshFileMetadata(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3])));
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public SegmentSourceId getSegmentSourceId()
    {
        return segmentSourceId;
    }

    // Support V9 index and version 1 index.drd only
    @Override
    public QueryableIndex loadIndex(List<ColumnHandle> columnHandles)
            throws IOException
    {
        int version = readVersion();
        if (version != V9_VERSION) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Expected version[9], got[%d]", version));
        }

        ByteBuffer indexBuffer = ByteBuffer.wrap(getColumnData(INDEX_METADATA_FILE_NAME));
        final GenericIndexed<String> allColumns = GenericIndexed.read(
                indexBuffer,
                GenericIndexed.STRING_STRATEGY);
        GenericIndexed<String> allDimensions = GenericIndexed.read(
                indexBuffer,
                STRING_STRATEGY);

        Interval dataInterval = Intervals.utc(indexBuffer.getLong(), indexBuffer.getLong());

        BitmapSerdeFactory segmentBitmapSerdeFactory;

        if (indexBuffer.hasRemaining()) {
            segmentBitmapSerdeFactory = JSON_MAPPER.readValue(SERIALIZER_UTILS.readString(indexBuffer), BitmapSerdeFactory.class);
        }
        else {
            segmentBitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();
        }

        Metadata metadata = null;
        ByteBuffer metadataBB = ByteBuffer.wrap(getColumnData(SEGMENT_METADATA_FILE_NAME));
        try {
            metadata = JSON_MAPPER.readValue(SERIALIZER_UTILS.readBytes(metadataBB, metadataBB.remaining()), Metadata.class);
        }
        catch (JsonParseException | JsonMappingException ex) {
            // Any jackson deserialization errors are ignored e.g. if metadata contains some aggregator which
            // is no longer supported then it is OK to not use the metadata instead of failing segment loading
            log.warn(ex, "Failed to load metadata for segment [%s]", segmentSourceId);
        }

        Map<String, ColumnHolder> columns = new HashMap<>();
        for (ColumnHandle columnHandle : columnHandles) {
            String columnName = ((DruidColumnHandle) columnHandle).getColumnName();
            columns.put(columnName, createColumnHolder(columnName));
        }

        List<String> availableDimensions = Streams.stream(allDimensions.iterator())
                .filter(dimension -> columns.containsKey(dimension))
                .collect(toImmutableList());

        columns.put(TIME_COLUMN_NAME, createColumnHolder(TIME_COLUMN_NAME));

        // TODO: get rid of the time column by creating Presto's SimpleQueryableIndex impl
        return new SimpleQueryableIndex(
                dataInterval,
                GenericIndexed.fromIterable(availableDimensions, STRING_STRATEGY),
                segmentBitmapSerdeFactory.getBitmapFactory(),
                columns,
                null,
                metadata);
    }

    private int readVersion()
    {
        int versionFileSize = (int) zipReader.getFileEntry(VERSION_FILE_NAME).getSize();
        byte[] buffer = new byte[versionFileSize];
        readOutFile(VERSION_FILE_NAME, 0, buffer);
        return ByteBuffer.wrap(buffer).getInt();
    }

    private ColumnDescriptor readColumnDescriptor(ByteBuffer byteBuffer)
            throws IOException
    {
        return JSON_MAPPER.readValue(SERIALIZER_UTILS.readString(byteBuffer), ColumnDescriptor.class);
    }

    private ColumnHolder createColumnHolder(String columnName)
            throws IOException
    {
        ByteBuffer columnData = ByteBuffer.wrap(getColumnData(columnName));
        ColumnDescriptor columnDescriptor = readColumnDescriptor(columnData);
        return columnDescriptor.read(columnData, () -> 0, null);
    }

    public void readOutFile(String outFileName, long position, byte[] buffer)
    {
        readOutFile(outFileName, position, buffer, 0, buffer.length);
    }

    private void readOutFile(String outFileName, long position, byte[] buffer, int offset, int length)
    {
        zipReader.readFully(outFileName, position, buffer, offset, length);
    }
}
