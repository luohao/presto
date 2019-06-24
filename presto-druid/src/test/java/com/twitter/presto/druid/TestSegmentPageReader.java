package com.twitter.presto.druid;

import com.twitter.presto.druid.segment.DataInputSourceId;
import com.twitter.presto.druid.segment.HdfsDataInputSource;
import com.twitter.presto.druid.segment.IndexFileSource;
import com.twitter.presto.druid.segment.SegmentColumnSource;
import com.twitter.presto.druid.segment.SegmentIndexSource;
import com.twitter.presto.druid.segment.SmooshedColumnSource;
import com.twitter.presto.druid.segment.V9SegmentIndexSource;
import com.twitter.presto.druid.segment.ZipIndexFileSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.IOException;

public class TestSegmentPageReader
{
    private static final String TEST_SEGMENT_PATH = "file:///tmp/druid/segments/wikipedia/20160627T000000.000Z_20160628T000000.000Z/2019-06-24T17_30_50.073Z/0_index.zip";

    @Test
    void testSegmentColumnSource()
            throws IOException
    {
        Path hdfsPath = new Path(TEST_SEGMENT_PATH);
        FileSystem fileSystem = hdfsPath.getFileSystem(new Configuration());
        long fileSize = fileSystem.getFileStatus(hdfsPath).getLen();
        FSDataInputStream inputStream = fileSystem.open(hdfsPath);
        DataInputSourceId dataInputSourceId = new DataInputSourceId(hdfsPath.toString());
        HdfsDataInputSource dataInputSource = new HdfsDataInputSource(dataInputSourceId, inputStream, fileSize);
        IndexFileSource indexFileSource = new ZipIndexFileSource(dataInputSource);
        SegmentColumnSource segmentColumnSource = new SmooshedColumnSource(indexFileSource);
        SegmentIndexSource segmentIndexSource = new V9SegmentIndexSource(segmentColumnSource);
    }
}
