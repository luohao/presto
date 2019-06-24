package com.twitter.presto.druid.segment;

import java.io.IOException;

public interface IndexFileSource
{
    byte[] readFile(String fileName)
            throws IOException;

    void readFile(String fileName, long position, byte[] buffer)
            throws IOException;
}
