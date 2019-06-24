package com.twitter.presto.druid.segment;

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.jackson.DefaultObjectMapper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import static com.twitter.presto.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;
import static com.twitter.presto.druid.DruidErrorCode.DRUID_ZIP_INDEX_FILE_ERROR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SmooshedColumnSource
        implements SegmentColumnSource
{
    private static final String FILE_EXTENSION = "smoosh";
    private static final String SMOOSH_METADATA_FILE_NAME = makeMetaFileName();
    private static final String VERSION_FILE_NAME = "version.bin";
    private static final String INDEX_METADATA_FILE_NAME = "index.drd";
    private static final String SEGMENT_METADATA_FILE_NAME = "metadata.drd";

    private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
    private static final SerializerUtils SERIALIZER_UTILS = new SerializerUtils();

    private final IndexFileSource indexFileSource;
    private final Map<String, SmooshFileMetadata> columnSmoosh = new TreeMap<>();

    private int numFiles;

    public SmooshedColumnSource(IndexFileSource indexFileSource)
    {
        this.indexFileSource = requireNonNull(indexFileSource, "indexFileSource is null");
        loadSmooshFileMetadata();
    }

    @Override
    public int getVersion()
    {
        try {
            return ByteBuffer.wrap(indexFileSource.readFile(FILE_EXTENSION)).getInt();
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, e);
        }
    }

    @Override
    public byte[] getColumnData(String name)
    {
        SmooshFileMetadata metadata = columnSmoosh.get(name);
        if (metadata == null) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Internal file %s doesn't exist", name));
        }
        String fileName = makeChunkFileName(metadata.getFileNum());
        int fileStart = metadata.getStartOffset();
        int fileSize = metadata.getEndOffset() - fileStart;

        byte[] buffer = new byte[fileSize];
        try {
            indexFileSource.readFile(fileName, fileStart, buffer);
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, e);
        }
        return buffer;
    }

    private void loadSmooshFileMetadata()
    {
        try {
            byte[] metadata = indexFileSource.readFile(SMOOSH_METADATA_FILE_NAME);
            BufferedReader in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(metadata)));
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
            numFiles = Integer.valueOf(splits[2]);

            while ((line = in.readLine()) != null) {
                splits = line.split(",");

                if (splits.length != 4) {
                    throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Malformed metadata file: wrong number of splits[%d] in line[%s]", splits.length, line));
                }
                columnSmoosh.put(splits[0].toLowerCase(ENGLISH), new SmooshFileMetadata(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3])));
            }
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, e);
        }
    }

    private static String makeMetaFileName()
    {
        return format("meta.%s", FILE_EXTENSION);
    }

    private static String makeChunkFileName(int i)
    {
        return format("%05d.%s", i, FILE_EXTENSION);
    }
}
