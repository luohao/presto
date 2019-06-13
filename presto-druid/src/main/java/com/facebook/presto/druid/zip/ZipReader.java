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
package com.facebook.presto.druid.zip;

import com.facebook.presto.druid.segment.IndexSource;
import com.facebook.presto.spi.PrestoException;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipException;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_ZIP_INDEX_FILE_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class ZipReader
        implements Closeable, AutoCloseable
{
    private final IndexSource indexSource;
    private final ZipFileData zipData;

    public ZipReader(IndexSource indexSource)
    {
        this.indexSource = requireNonNull(indexSource, "indexSource is null");
        this.zipData = requireNonNull(readCentralDirectory(), "zip data is null");
    }

    /**
     * Reads file inside a zip archive
     */
    public final void readFully(String fileName, long position, byte[] buffer)
    {
        readFully(fileName, position, buffer, 0, buffer.length);
    }

    public final void readFully(String fileName, long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        ZipFileEntry entry = zipData.getEntry(fileName);
        if (entry == null) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Zip doesn't contain file: %s", fileName));
        }

        readFully(entry, position, buffer, bufferOffset, bufferLength);
    }

    public final void readFully(ZipFileEntry entry, long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        long offset = entry.getLocalHeaderOffset();
        byte[] fileHeader = new byte[LocalFileHeader.FIXED_DATA_SIZE];
        indexSource.readFully(offset, fileHeader);
        offset += fileHeader.length;

        if (!ZipUtil.arrayStartsWith(fileHeader, ZipUtil.intToLittleEndian(LocalFileHeader.SIGNATURE))) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR,
                    format("The file '%s' is not a correctly formatted zip file: Expected a File Header at file offset %d, but was not present.", entry.getName()));
        }

        // skip name and extra field
        int nameLength = ZipUtil.getUnsignedShort(fileHeader, LocalFileHeader.FILENAME_LENGTH_OFFSET);
        int extraFieldLength = ZipUtil.getUnsignedShort(fileHeader, LocalFileHeader.EXTRA_FIELD_LENGTH_OFFSET);
        offset += (nameLength + extraFieldLength);

        // deflate
        int compressedSize = (int) entry.getCompressedSize();
        byte[] compressedData = new byte[compressedSize];
        indexSource.readFully(offset, compressedData);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(new ByteArrayInputStream(compressedData), new Inflater(true));

        try {
            inflaterInputStream.skip(position);
            int size = chunkedRead(inflaterInputStream, buffer);
            checkArgument(size == bufferLength);
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Malformed zip file: %s", entry.getName()));
        }
    }

    // read inflater stream chunk by chunk
    private static int chunkedRead(InflaterInputStream inflaterInputStream, byte[] buffer)
            throws IOException
    {
        return chunkedRead(inflaterInputStream, buffer, 0, buffer.length);
    }

    private static int chunkedRead(InflaterInputStream inflaterInputStream, byte[] buffer, int offset, int length)
            throws IOException
    {
        int position = offset;
        int bytesRead = 0;

        while (((position - offset) < length) && bytesRead != -1) {
            bytesRead = inflaterInputStream.read(buffer, position, offset + length - position);
            if (bytesRead > 0) {
                position += bytesRead;
            }
        }

        return position - offset;
    }

    public final ZipFileEntry getFileEntry(String fileName)
    {
        return zipData.getEntry(fileName);
    }

    public Collection<ZipFileEntry> getFileEntry()
    {
        return zipData.getEntries();
    }

    /**
     * Finds, reads and parses ZIP file entries from the central directory.
     */
    private ZipFileData readCentralDirectory()
    {
        long eocdLocation = findEndOfCentralDirectoryRecord();
        ZipFileData fileData = new ZipFileData(UTF_8);
        EndOfCentralDirectoryRecord.read(fileData, indexSource, eocdLocation);

        if (fileData.isMaybeZip64()) {
            try {
                Zip64EndOfCentralDirectoryLocator.read(fileData, indexSource, eocdLocation - Zip64EndOfCentralDirectoryLocator.FIXED_DATA_SIZE);
                Zip64EndOfCentralDirectory.read(fileData, indexSource, fileData.getZip64EndOfCentralDirectoryOffset());
            }
            catch (ZipException e) {
                // expected if not in Zip64 format
            }
        }

        if (fileData.isZip64()) {
            // If in Zip64 format or using strict entry numbers, use the parsed information as is to read
            // the central directory file headers.
            readCentralDirectoryFileHeaders(fileData, indexSource, fileData.getCentralDirectoryOffset(), fileData.getCharset(), fileData.getExpectedEntries());
        }
        else {
            // If not in Zip64 format, compute central directory offset by end of central directory record
            // offset and central directory size to allow reading large non-compliant Zip32 directories.
            long centralDirectoryOffset = eocdLocation - fileData.getCentralDirectorySize();
            // If the lower 4 bytes match, the above calculation is correct; otherwise fallback to
            // reported offset.
            if ((int) centralDirectoryOffset == (int) fileData.getCentralDirectoryOffset()) {
                readCentralDirectoryFileHeaders(fileData, indexSource, centralDirectoryOffset, fileData.getCharset());
            }
            else {
                readCentralDirectoryFileHeaders(fileData, indexSource, fileData.getCentralDirectoryOffset(), fileData.getCharset(), fileData.getExpectedEntries());
            }
        }
        return fileData;
    }

    /**
     * Finds the file offset of the end of central directory record.
     */
    private long findEndOfCentralDirectoryRecord()
    {
        long fileSize = indexSource.getSize();
        byte[] signature = ZipUtil.intToLittleEndian(EndOfCentralDirectoryRecord.SIGNATURE);
        byte[] buffer = new byte[(int) Math.min(64, fileSize)];
        int readLength = buffer.length;
        if (readLength < EndOfCentralDirectoryRecord.FIXED_DATA_SIZE) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Zip file '%s' is malformed. It does not contain an end of central directory record.", indexSource.getId()));
        }

        long offset = fileSize - buffer.length;
        while (offset >= 0) {
            indexSource.readFully(offset, buffer, 0, readLength);
            int signatureLocation = scanBackwards(signature, buffer, buffer.length);
            while (signatureLocation != -1) {
                long eocdSize = fileSize - offset - signatureLocation;
                if (eocdSize >= EndOfCentralDirectoryRecord.FIXED_DATA_SIZE) {
                    int commentLength = ZipUtil.getUnsignedShort(buffer, signatureLocation
                            + EndOfCentralDirectoryRecord.COMMENT_LENGTH_OFFSET);
                    long readCommentLength = eocdSize - EndOfCentralDirectoryRecord.FIXED_DATA_SIZE;
                    if (commentLength == readCommentLength) {
                        return offset + signatureLocation;
                    }
                }
                signatureLocation = scanBackwards(signature, buffer, signatureLocation - 1);
            }
            readLength = buffer.length - 3;
            buffer[buffer.length - 3] = buffer[0];
            buffer[buffer.length - 2] = buffer[1];
            buffer[buffer.length - 1] = buffer[2];
            offset -= readLength;
        }
        throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Zip file '%s' is malformed. It does not contain an end of central directory record.", indexSource.getId()));
    }

    /**
     * Reads and parses ZIP file entries from the central directory.
     */
    private void readCentralDirectoryFileHeaders(ZipFileData fileData, IndexSource indexSource, long fileOffset, Charset charset, long count)
    {
        try {
            long position = fileOffset;
            for (long i = 0; i < count; ++i) {
                position += CentralDirectoryFileHeader.read(fileData, indexSource, position, charset);
            }
        }
        catch (ZipException e) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, e);
        }
    }

    /**
     * Reads and parses ZIP file entries from the central directory.
     */
    private void readCentralDirectoryFileHeaders(ZipFileData fileData, IndexSource indexSource, long fileOffset, Charset charset)
    {
        try {
            long position = fileOffset;
            while ((position - fileOffset) < fileData.getCentralDirectorySize()) {
                position += CentralDirectoryFileHeader.read(fileData, indexSource, position, charset);
            }
        }
        catch (ZipException e) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, e);
        }
    }

    /**
     * Looks for the target sub array in the buffer scanning backwards starting at offset. Returns the
     * index where the target is found or -1 if not found.
     */
    private int scanBackwards(byte[] target, byte[] buffer, int offset)
    {
        int start = Math.min(offset, buffer.length - target.length);
        for (int i = start; i >= 0; i--) {
            for (int j = 0; j < target.length; j++) {
                if (buffer[i + j] != target[j]) {
                    break;
                }
                else if (j == target.length - 1) {
                    return i;
                }
            }
        }
        return -1;
    }

    @Override
    public void close()
            throws IOException
    {
        indexSource.close();
    }
}
