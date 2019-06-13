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

import java.util.zip.ZipException;

class Zip64EndOfCentralDirectoryLocator
{
    static final int SIGNATURE = 0x07064b50;
    static final int FIXED_DATA_SIZE = 20;
    static final int SIGNATURE_OFFSET = 0;
    static final int ZIP64_EOCD_DISK_OFFSET = 4;
    static final int ZIP64_EOCD_OFFSET_OFFSET = 8;
    static final int DISK_NUMBER_OFFSET = 16;

    private Zip64EndOfCentralDirectoryLocator()
    {
    }

    /**
     * Read the Zip64 end of central directory locator from the input stream and parse additional
     * {@link ZipFileData} from it.
     */
    static ZipFileData read(ZipFileData file, IndexSource indexSource, long position)
            throws ZipException
    {
        if (file == null) {
            throw new NullPointerException();
        }

        byte[] fixedSizeData = new byte[FIXED_DATA_SIZE];
        indexSource.readFully(position, fixedSizeData, 0, FIXED_DATA_SIZE);
        if (!ZipUtil.arrayStartsWith(fixedSizeData, ZipUtil.intToLittleEndian(SIGNATURE))) {
            throw new ZipException(String.format(
                    "Malformed Zip64 Central Directory Locator; does not start with %08x", SIGNATURE));
        }
        file.setZip64(true);
        file.setZip64EndOfCentralDirectoryOffset(ZipUtil.getUnsignedLong(fixedSizeData, ZIP64_EOCD_OFFSET_OFFSET));
        return file;
    }
}
