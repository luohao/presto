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

import static java.lang.String.format;

public class FileSmoosher
{
    private static final String FILE_EXTENSION = "smoosh";

    private FileSmoosher()
    {
    }

    public static String makeMetaFileName()
    {
        return format("meta.%s", FILE_EXTENSION);
    }

    public static String makeChunkFileName(int i)
    {
        return format("%05d.%s", i, FILE_EXTENSION);
    }
}
