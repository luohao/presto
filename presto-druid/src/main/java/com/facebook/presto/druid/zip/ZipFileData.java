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
// Copyright 2015 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.facebook.presto.druid.zip;

import com.facebook.presto.spi.PrestoException;

import javax.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_ZIP_INDEX_FILE_ERROR;
import static com.facebook.presto.druid.zip.ZipFileEntry.Feature;
import static java.lang.String.format;

/**
 * A representation of a ZIP file. Contains the file comment, encoding, and entries. Also contains
 * internal information about the structure and location of ZIP file parts.
 */
class ZipFileData
{
    private final Charset charset;
    private String comment;

    private long centralDirectorySize;
    private long centralDirectoryOffset;
    private long expectedEntries;
    private long numEntries;
    private final Map<String, ZipFileEntry> entries;

    private boolean maybeZip64;
    private boolean isZip64;
    private long zip64EndOfCentralDirectoryOffset;

    public ZipFileData(Charset charset)
    {
        if (charset == null) {
            throw new NullPointerException();
        }
        this.charset = charset;
        comment = "";
        entries = new LinkedHashMap<>();
    }

    public Charset getCharset()
    {
        return charset;
    }

    public String getComment()
    {
        return comment;
    }

    public void setComment(byte[] comment)
    {
        if (comment == null) {
            throw new NullPointerException();
        }
        if (comment.length > 0xffff) {
            throw new PrestoException(DRUID_ZIP_INDEX_FILE_ERROR, format("File comment too long. Is %d; max %d.", comment.length, 0xffff));
        }
        this.comment = fromBytes(comment);
    }

    public void setComment(String comment)
    {
        setComment(getBytes(comment));
    }

    public long getCentralDirectorySize()
    {
        return centralDirectorySize;
    }

    public void setCentralDirectorySize(long centralDirectorySize)
    {
        this.centralDirectorySize = centralDirectorySize;
        if (centralDirectorySize > 0xffffffffL) {
            setZip64(true);
        }
    }

    public long getCentralDirectoryOffset()
    {
        return centralDirectoryOffset;
    }

    public void setCentralDirectoryOffset(long offset)
    {
        this.centralDirectoryOffset = offset;
        if (centralDirectoryOffset > 0xffffffffL) {
            setZip64(true);
        }
    }

    public long getExpectedEntries()
    {
        return expectedEntries;
    }

    public void setExpectedEntries(long count)
    {
        this.expectedEntries = count;
        if (expectedEntries > 0xffff) {
            setZip64(true);
        }
    }

    public long getNumEntries()
    {
        return numEntries;
    }

    private void setNumEntries(long numEntries)
    {
        this.numEntries = numEntries;
        if (numEntries > 0xffff) {
            setZip64(true);
        }
    }

    public Collection<ZipFileEntry> getEntries()
    {
        return entries.values();
    }

    public ZipFileEntry getEntry(@Nullable String name)
    {
        return entries.get(name);
    }

    public void addEntry(ZipFileEntry entry)
    {
        entries.put(entry.getName(), entry);
        setNumEntries(numEntries + 1);
        if (entry.getFeatureSet().contains(Feature.ZIP64_SIZE)
                || entry.getFeatureSet().contains(Feature.ZIP64_CSIZE)
                || entry.getFeatureSet().contains(Feature.ZIP64_OFFSET)) {
            setZip64(true);
        }
    }

    public boolean isMaybeZip64()
    {
        return maybeZip64;
    }

    public void setMaybeZip64(boolean maybeZip64)
    {
        this.maybeZip64 = maybeZip64;
    }

    public boolean isZip64()
    {
        return isZip64;
    }

    public void setZip64(boolean isZip64)
    {
        this.isZip64 = isZip64;
        setMaybeZip64(true);
    }

    public long getZip64EndOfCentralDirectoryOffset()
    {
        return zip64EndOfCentralDirectoryOffset;
    }

    public void setZip64EndOfCentralDirectoryOffset(long offset)
    {
        this.zip64EndOfCentralDirectoryOffset = offset;
        setZip64(true);
    }

    public byte[] getBytes(String string)
    {
        return string.getBytes(charset);
    }

    public String fromBytes(byte[] bytes)
    {
        return new String(bytes, charset);
    }
}
