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
package com.twitter.presto.druid.metadata;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

import static java.util.Objects.requireNonNull;

public enum DruidColumnType
{
    STRING(VarcharType.VARCHAR),
    LONG(BigintType.BIGINT),
    FLOAT(RealType.REAL),
    DOUBLE(DoubleType.DOUBLE);

    private final Type type;

    DruidColumnType(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    public Type getType()
    {
        return type;
    }
}
