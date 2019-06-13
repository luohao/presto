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

import static com.facebook.presto.druid.DruidErrorCode.DRUID_ZIP_INDEX_FILE_ERROR;
import static java.lang.String.format;

class EndOfCentralDirectoryRecord
{
    static final int SIGNATURE = 0x06054b50;
    static final int FIXED_DATA_SIZE = 22;
    static final int SIGNATURE_OFFSET = 0;
    static final int DISK_NUMBER_OFFSET = 4;
    static final int CD_DISK_OFFSET = 6;
    static final int DISK_ENTRIES_OFFSET = 8;
    static final int TOTAL_ENTRIES_OFFSET = 10;
    static final int CD_SIZE_OFFSET = 12;
    static final int CD_OFFSET_OFFSET = 16;
    static final int COMMENT_LENGTH_OFFSET = 20;

    private EndOfCentralDirectoryRecord()
    {
    }

    /**
     * Read the end of central directory record from the input stream and parse {@link ZipFileData}
     * from it.
     */
    static void read(ZipFileData zipFileData, IndexSource indexSource, long offset)
    {
        long position = offset;
        byte[] fixedSizeData = new byte[FIXED_DATA_SIZE];

        indexSource.readFully(position, fixedSizeData, 0, FIXED_DATA_SIZE);
        position += FIXED_DATA_SIZE;
        if (!ZipUtil.arrayStartsWith(fixedSizeData, ZipUtil.intToLittleEndian(SIGNATURE))) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("Malformed End of Central Directory Record; does not start with %08x", SIGNATURE));
        }

        byte[] comment = new byte[ZipUtil.getUnsignedShort(fixedSizeData, COMMENT_LENGTH_OFFSET)];
        if (comment.length > 0) {
            indexSource.readFully(position, comment, 0, comment.length);
        }
        short diskNumber = ZipUtil.get16(fixedSizeData, DISK_NUMBER_OFFSET);
        short centralDirectoryDisk = ZipUtil.get16(fixedSizeData, CD_DISK_OFFSET);
        short entriesOnDisk = ZipUtil.get16(fixedSizeData, DISK_ENTRIES_OFFSET);
        short totalEntries = ZipUtil.get16(fixedSizeData, TOTAL_ENTRIES_OFFSET);
        int centralDirectorySize = ZipUtil.get32(fixedSizeData, CD_SIZE_OFFSET);
        int centralDirectoryOffset = ZipUtil.get32(fixedSizeData, CD_OFFSET_OFFSET);
        if (diskNumber == -1 || centralDirectoryDisk == -1 || entriesOnDisk == -1
                || totalEntries == -1 || centralDirectorySize == -1 || centralDirectoryOffset == -1) {
            zipFileData.setMaybeZip64(true);
        }
        zipFileData.setComment(comment);
        zipFileData.setCentralDirectorySize(ZipUtil.getUnsignedInt(fixedSizeData, CD_SIZE_OFFSET));
        zipFileData.setCentralDirectoryOffset(ZipUtil.getUnsignedInt(fixedSizeData, CD_OFFSET_OFFSET));
        zipFileData.setExpectedEntries(ZipUtil.getUnsignedShort(fixedSizeData, TOTAL_ENTRIES_OFFSET));
    }

    static byte[] create(ZipFileData file, boolean allowZip64)
    {
        throw new UnsupportedOperationException();
    }
}
