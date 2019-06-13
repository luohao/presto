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
package com.facebook.presto.druid.util;

import com.facebook.presto.hadoop.HadoopFileSystemCache;
import com.facebook.presto.hadoop.HadoopNative;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

// TODO: support impersonation & context
public class HdfsUtil
{
    static {
        HadoopNative.requireHadoopNative();
        HadoopFileSystemCache.initialize();
    }

    private static final Configuration INITIAL_CONFIGURATION = new Configuration();
    private final ThreadLocal<Configuration> hdfsConfiguration = ThreadLocal.withInitial(() -> {
        Configuration configuration = new Configuration(false);
        copy(INITIAL_CONFIGURATION, configuration);
        return configuration;
    });

    public Configuration getConfiguration()
    {
        return hdfsConfiguration.get();
    }

    public FileSystem getFileSystem(Path path)
            throws IOException
    {
        return path.getFileSystem(getConfiguration());
    }

    public FSDataInputStream getInputStream(Path path)
            throws IOException
    {
        return getFileSystem(path).open(path);
    }

    public static void copy(Configuration from, Configuration to)
    {
        for (Map.Entry<String, String> entry : from) {
            to.set(entry.getKey(), entry.getValue());
        }
    }
}
