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
package com.facebook.presto.druid;

//import com.facebook.presto.druid.segment.HdfsIndexSource;
//import com.facebook.presto.druid.segment.SegmentSourceId;
//import com.facebook.presto.druid.segment.ZippedSegment;
//import com.facebook.presto.druid.zip.ZipFileEntry;
//import com.facebook.presto.druid.zip.ZipReader;
//import com.google.common.collect.ImmutableList;
//import java.nio.file.Files;
//import org.apache.druid.jackson.DefaultObjectMapper;
//import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
//import org.apache.druid.segment.ColumnValueSelector;
//import org.apache.druid.segment.IndexIO;
//import org.apache.druid.segment.QueryableIndex;
//import org.apache.druid.segment.data.ReadableOffset;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.testng.annotations.Test;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.security.MessageDigest;
//import java.security.NoSuchAlgorithmException;
//import java.util.List;
//import java.util.stream.IntStream;
//import java.util.zip.ZipEntry;
//import java.util.zip.ZipFile;
//import java.util.zip.ZipInputStream;
//
//import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
//import static com.google.common.collect.ImmutableList.toImmutableList;
//import static java.lang.String.format;
//import static org.apache.druid.java.util.common.CompressionUtils.unzip;
//import static org.testng.Assert.assertEquals;

public class TestSegmentReader
{
//    @Test
//    void testLocalStorage()
//            throws IOException
//    {
//        final String path = "/Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segment-cache/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00.000Z/2019-06-05T06:07:33.141Z/0";
//        IndexIO indexIO = new IndexIO(new DefaultObjectMapper(), () -> 0);
//        QueryableIndex queryableIndex = indexIO.loadIndex(new File(path));
//
//        ColumnValueSelector<?> valueSelector = queryableIndex.getColumnHolder("diffUrl").getColumn().makeColumnValueSelector(new ReadableOffset()
//        {
//            private int row;
//
//            @Override
//            public int getOffset()
//            {
//                int offset = row;
//                ++row;
//                return offset;
//            }
//
//            @Override
//            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
//            {
//            }
//        });
//
//        List<String> results = IntStream.range(0, queryableIndex.getNumRows()).mapToObj(i -> (String) valueSelector.getObject()).collect(toImmutableList());
//    }
//
//    @Test
//    void testZipFile()
//            throws IOException
//    {
//        final String name = "file:///Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segments/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
//                ".000Z/2019-06-05T06:07:33" +
//                ".141Z/0/index.zip";
//        Path path = new Path(name);
//        FileSystem fileSystem = FileSystem.get(new Configuration());
//        FSDataInputStream inputStream = fileSystem.open(path);
//        FileStatus fileStatus = fileSystem.getFileStatus(path);
//        HdfsIndexSource indexSource = new HdfsIndexSource(new SegmentSourceId(name), inputStream, fileStatus.getLen());
//        ZipReader zipReader = new ZipReader(indexSource);
//        ZipFileEntry fileEntry = zipReader.getFileEntry("meta.smoosh");
//        byte[] buffer = new byte[(int) fileEntry.getSize()];
//        zipReader.readFully(fileEntry.getName(), 0, buffer);
//        System.out.println(new String(buffer));
//    }
//
//    @Test
//    void testSegment()
//            throws IOException
//    {
//        final String name = "file:///Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segments/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
//                ".000Z/2019-06-05T06:07:33" +
//                ".141Z/0/index.zip";
//        Path path = new Path(name);
//        FileSystem fileSystem = FileSystem.get(new Configuration());
//        FSDataInputStream inputStream = fileSystem.open(path);
//        FileStatus fileStatus = fileSystem.getFileStatus(path);
//        HdfsIndexSource indexSource = new HdfsIndexSource(new SegmentSourceId(name), inputStream, fileStatus.getLen());
//        ZippedSegment segment = new ZippedSegment(new SegmentSourceId(name), indexSource);
//        QueryableIndex queryableIndex = segment.loadIndex(ImmutableList.of(new DruidColumnHandle("diffUrl", VARCHAR)));
//
//        ColumnValueSelector<?> valueSelector = queryableIndex.getColumnHolder("diffUrl").getColumn().makeColumnValueSelector(new ReadableOffset()
//        {
//            private int row;
//
//            @Override
//            public int getOffset()
//            {
//                int offset = row;
//                ++row;
//                return offset;
//            }
//
//            @Override
//            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
//            {
//            }
//        });
//
//        List<String> results = IntStream.range(0, queryableIndex.getNumRows()).mapToObj(i -> (String) valueSelector.getObject()).collect(toImmutableList());
//        System.out.println(results);
//    }
//
////    @Test
////    void verifyZipReader()
////            throws IOException
////    {
////        String zipFileName = "/Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segments/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
////                ".000Z/2019-06-05T06:07:33" +
////                ".141Z/0/index.zip";
////        String hdfsZipFileName = "file:///Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segments/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
////                ".000Z/2019-06-05T06:07:33" +
////                ".141Z/0/index.zip";
////        List<String> filenames = ImmutableList.of("factory.json", "meta.smoosh", "version.bin", "00000.smoosh");
////        com.facebook.presto.druid.testing.reference.ZipReader expectedZipReader = new com.facebook.presto.druid.testing.reference.ZipReader(new File(zipFileName));
////
////        Path path = new Path(zipFileName);
////        FileSystem fileSystem = FileSystem.get(new Configuration());
////        FSDataInputStream inputStream = fileSystem.open(path);
////        FileStatus fileStatus = fileSystem.getFileStatus(path);
////        HdfsIndexSource indexSource = new HdfsIndexSource(new SegmentSourceId(hdfsZipFileName), inputStream, fileStatus.getLen());
////        ZipReader zipReader = new ZipReader(indexSource);
////
////        for (String name : filenames) {
////            com.facebook.presto.druid.testing.reference.ZipFileEntry expectedEntry = expectedZipReader.getEntry(name);
////            InputStream expectedInputStream = expectedZipReader.getInputStream(expectedEntry);
////            byte[] expected = new byte[(int) expectedEntry.getSize()];
////            expectedInputStream.read(expected);
////
////            ZipFileEntry fileEntry = zipReader.getFileEntry(name);
////            byte[] actual = new byte[(int) fileEntry.getSize()];
////            zipReader.readFully(fileEntry.getName(), 0, actual);
////            System.out.println(format("=== Checking %s ===", name));
////            assertEquals(expected.length, actual.length);
////            for (int i = 0; i < actual.length; ++i) {
////                assertEquals(expected[i], actual[i], format("byte[%d] mismatch", i));
////            }
////            System.out.println(format("=== Verfied %s ===\n", name));
////        }
////    }
////
////    @Test
////    void verifyZipReader2()
////            throws IOException
////    {
////        String zipFileName = "/Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segments/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
////                ".000Z/2019-06-05T06:07:33" +
////                ".141Z/0/index.zip";
////        String hdfsZipFileName = "file:///Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segments/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
////                ".000Z/2019-06-05T06:07:33" +
////                ".141Z/0/index.zip";
////        List<String> filenames = ImmutableList.of("factory.json", "meta.smoosh", "version.bin", "00000.smoosh");
////
////        ZipFile expectedZipFile = new ZipFile(new File(zipFileName));
////
////
////        Path path = new Path(zipFileName);
////        FileSystem fileSystem = FileSystem.get(new Configuration());
////        FSDataInputStream inputStream = fileSystem.open(path);
////        FileStatus fileStatus = fileSystem.getFileStatus(path);
////        HdfsIndexSource indexSource = new HdfsIndexSource(new SegmentSourceId(hdfsZipFileName), inputStream, fileStatus.getLen());
////        ZipReader zipReader = new ZipReader(indexSource);
////
////        for (String name : filenames) {
////            com.facebook.presto.druid.testing.reference.ZipFileEntry expectedEntry = expectedZipReader.getEntry(name);
////            InputStream expectedInputStream = expectedZipReader.getInputStream(expectedEntry);
////            byte[] expected = new byte[(int) expectedEntry.getSize()];
////            expectedInputStream.read(expected);
////
////            ZipFileEntry fileEntry = zipReader.getFileEntry(name);
////            byte[] actual = new byte[(int) fileEntry.getSize()];
////            zipReader.readFully(fileEntry.getName(), 0, actual);
////            System.out.println(format("=== Checking %s ===", name));
////            assertEquals(expected.length, actual.length);
////            for (int i = 0; i < actual.length; ++i) {
////                assertEquals(expected[i], actual[i], format("byte[%d] mismatch", i));
////            }
////            System.out.println(format("=== Verfied %s ===\n", name));
////        }
////    }
//
//    @Test
//    void verifyZipFile()
//            throws IOException
//    {
//        String zipFileName = "/Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segments/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
//                ".000Z/2019-06-05T06:07:33" +
//                ".141Z/0/index.zip";
//
//        String extractedDir = "/Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segment-cache/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
//                ".000Z/2019-06-05T06:07:33.141Z/0/";
//
//        ZipFile zipFile = new ZipFile(new File(zipFileName));
//        zipFile.stream()
//                .forEach(entry -> {
//                    try {
//                        String name = ((ZipEntry) entry).getName();
//                        byte[] expected = Files.readAllBytes(new File(extractedDir + name));
//                        byte[] actual = new byte[(int) ((ZipEntry) entry).getSize()];
//                        zipFile.getInputStream(entry).read(actual);
//
//                        System.out.println(format("=== Checking %s ===", name));
//                        assertEquals(expected.length, actual.length);
//                        System.out.println(format("=== Size %d ===\n", actual.length));
//                        for (int i = 0; i < actual.length; ++i) {
//                            assertEquals(expected[i], actual[i], format("byte[%d] mismatch", i));
//                        }
//                        System.out.println(format("=== Verfied %s ===\n", name));
//                    }
//                    catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                });
//    }
//
//    @Test
//    void verifyExtractedContent()
//            throws IOException, NoSuchAlgorithmException
//    {
//        String zipFileName = "file:///Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segments/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
//                ".000Z/2019-06-05T06:07:33" +
//                ".141Z/0/index.zip";
//
//        String extractedDir = "/Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segment-cache/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
//                ".000Z/2019-06-05T06:07:33.141Z/0/";
//
////        List<String> filenames = ImmutableList.of("factory.json", "meta.smoosh", "version.bin", "00000.smoosh");
//        List<String> filenames = ImmutableList.of("00000.smoosh");
//
//        Path path = new Path(zipFileName);
//        FileSystem fileSystem = FileSystem.get(new Configuration());
//        FSDataInputStream inputStream = fileSystem.open(path);
//        FileStatus fileStatus = fileSystem.getFileStatus(path);
//        HdfsIndexSource indexSource = new HdfsIndexSource(new SegmentSourceId(zipFileName), inputStream, fileStatus.getLen());
//        ZipReader zipReader = new ZipReader(indexSource);
//
//        for (String name : filenames) {
//            byte[] expected = Files.readAllBytes(new File(extractedDir + name));
//            ZipFileEntry fileEntry = zipReader.getFileEntry(name);
//            byte[] actual = new byte[(int) fileEntry.getSize()];
//            zipReader.readFully(fileEntry.getName(), 0, actual);
//            System.out.println(format("=== Checking %s ===", name));
//            assertEquals(expected.length, actual.length);
//            System.out.println(format("=== Size %d ===\n", actual.length));
//            MessageDigest md = MessageDigest.getInstance("MD5");
//            md.update(actual);
//            for (int i = 0; i < actual.length; ++i) {
//                assertEquals(expected[i], actual[i], format("byte[%d] mismatch", i));
//            }
//            System.out.println(format("=== Verfied %s ===\n", name));
//        }
//    }
//
//    @Test
//    void verifyInputStream()
//            throws IOException
//    {
//        String zipFileName = "/Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segments/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
//                ".000Z/2019-06-05T06:07:33" +
//                ".141Z/0/index.zip";
//
//        String extractedDir = "/Users/hluo/workspace/druid/test/imply-2.9.12/var/druid/segment-cache/wikipedia/2016-06-27T00:00:00.000Z_2016-06-28T00:00:00" +
//                ".000Z/2019-06-05T06:07:33.141Z/0/";
//
//        try (final ZipInputStream zipIn = new ZipInputStream(new FileInputStream(new File(zipFileName)))) {
//            ZipEntry entry;
//            while ((entry = zipIn.getNextEntry()) != null) {
//                byte[] expected = Files.toByteArray(new File(extractedDir, entry.getName()));
//                byte[] buf = new byte[expected.length * 2];
//                int numBytes = 0;
//                for (int bytesRead = 0; bytesRead > -1; ) {
//                    bytesRead = zipIn.read(buf, numBytes, 128);
//                    if (bytesRead > 0) {
//                        numBytes += bytesRead;
//                    }
//                }
//                assertEquals(numBytes, expected.length);
//
//                System.out.println(format("=== Checking %s ===", entry.getName()));
////                assertEquals(expected.length, buf.length);
//                System.out.println(format("=== Size %d ===\n", buf.length));
//                for (int i = 0; i < expected.length; ++i) {
//                    assertEquals(expected[i], buf[i], format("byte[%d] mismatch", i));
//                }
//                System.out.println(format("=== Verfied %s ===\n", entry.getName()));
//            }
//        }
//    }
//
//    @Test
//    void verifyUnzip()
//            throws IOException
//    {
//        String zipFileName = "/tmp/index.zip";
//
//        String extractedDir = "/tmp/index/";
//
//        try (final InputStream in = new FileInputStream(zipFileName)) {
//            unzip(in, new File("/tmp/ooo/"));
//        }
//    }
}
