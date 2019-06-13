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

class LocalFileHeader
{
    static final int SIGNATURE = 0x04034b50;
    static final int FIXED_DATA_SIZE = 30;
    static final int SIGNATURE_OFFSET = 0;
    static final int VERSION_OFFSET = 4;
    static final int FLAGS_OFFSET = 6;
    static final int METHOD_OFFSET = 8;
    static final int MOD_TIME_OFFSET = 10;
    static final int CRC_OFFSET = 14;
    static final int COMPRESSED_SIZE_OFFSET = 18;
    static final int UNCOMPRESSED_SIZE_OFFSET = 22;
    static final int FILENAME_LENGTH_OFFSET = 26;
    static final int EXTRA_FIELD_LENGTH_OFFSET = 28;
    static final int VARIABLE_DATA_OFFSET = 30;

    private LocalFileHeader()
    {
    }
}
