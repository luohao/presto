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
package com.twitter.presto.druid;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.twitter.presto.druid.metadata.DruidColumnInfo;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.twitter.presto.druid.DruidTableHandle.fromSchemaTableName;
import static java.util.Objects.requireNonNull;

public class DruidMetadata
        implements ConnectorMetadata
{
    private static final String DRUID_SCHEMA = "druid";

    private final DruidClient druidClient;

    @Inject
    public DruidMetadata(DruidClient druidClient)
    {
        this.druidClient = requireNonNull(druidClient, "druidClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(DRUID_SCHEMA);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return druidClient.getTables().stream()
                .map(tableName -> new SchemaTableName(DRUID_SCHEMA, tableName))
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return druidClient.getTables().stream()
                .filter(name -> name.equals(tableName.getTableName()))
                .map(name -> new DruidTableHandle(DRUID_SCHEMA, name))
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        DruidTableHandle tableHandle = (DruidTableHandle) table;
        List<String> segments = druidClient.getDataSegmentIds(tableHandle.getTableName());
        ConnectorTableLayout layout = new ConnectorTableLayout(new DruidTableLayoutHandle(((DruidTableHandle) table).toSchemaTableName(), segments));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DruidTableHandle druidTable = (DruidTableHandle) tableHandle;
        List<ColumnMetadata> columns = druidClient.getColumnDataType(druidTable.getTableName()).stream()
                .map(column -> toColumnMetadata(column))
                .collect(toImmutableList());

        return new ConnectorTableMetadata(druidTable.toSchemaTableName(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DruidTableHandle druidTable = (DruidTableHandle) tableHandle;
        return druidClient.getColumnDataType(druidTable.getTableName()).stream()
                .collect(toImmutableMap(DruidColumnInfo::getColumnName, column -> toColumnHandle(column)));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DruidTableHandle druidTable = (DruidTableHandle) tableHandle;
        DruidColumnHandle druidColumn = (DruidColumnHandle) columnHandle;

        return druidClient.getColumnDataType(druidTable.getTableName()).stream()
                .filter(column -> column.getColumnName().equals(druidColumn.getColumnName()))
                .map(DruidMetadata::toColumnMetadata)
                .findFirst()
                .orElse(null);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, fromSchemaTableName(tableName));
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, Optional.of(prefix.getSchemaName()));
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    private static Type getType(String type)
    {
        switch (type.toUpperCase()) {
            case "VARCHAR":
                return VARCHAR;
            case "BIGINT":
                return BIGINT;
            case "FLOAT":
                return REAL;
            case "DOUBLE":
                return DOUBLE;
            case "TIMESTAMP":
                return TIMESTAMP;
            default:
                throw new IllegalArgumentException("unsupported type: " + type);
        }
    }

    private static ColumnMetadata toColumnMetadata(DruidColumnInfo column)
    {
        return new ColumnMetadata(column.getColumnName(), getType(column.getDataType()));
    }

    private static ColumnHandle toColumnHandle(DruidColumnInfo column)
    {
        return new DruidColumnHandle(column.getColumnName(), getType(column.getDataType()));
    }
}
