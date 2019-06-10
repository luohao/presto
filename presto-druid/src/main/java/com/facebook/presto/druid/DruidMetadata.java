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
package com.facebook.presto.druid;

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
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.druid.DruidTableHandle.fromSchemaTableName;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
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
        // According to Druid SQL specification, all datasources will be in druid schema
        return ImmutableList.of(DRUID_SCHEMA);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return druidClient.getDataSources(true).stream()
                .map(datasource -> new SchemaTableName(DRUID_SCHEMA, datasource))
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return fromSchemaTableName(tableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        throw new UnsupportedOperationException(format("Unimplemented method: %s", new Object().getClass().getEnclosingClass().getName()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        throw new UnsupportedOperationException(format("Unimplemented method: %s", new Object().getClass().getEnclosingClass().getName()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        //throw new UnsupportedOperationException(format("Unimplemented method: %s", new Object().getClass().getEnclosingClass().getName()));
        DruidTableHandle druidTable = (DruidTableHandle) tableHandle;
        return toTableMetadata(druidTable.toSchemaTableName(), druidClient.getAllSegmentMetadata(druidTable.getTableName()));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        //throw new UnsupportedOperationException(format("Unimplemented method: %s", new Object().getClass().getEnclosingClass().getName()));
        DruidTableHandle druidTable = (DruidTableHandle) tableHandle;
        ConnectorTableMetadata tableMetadata = toTableMetadata(druidTable.toSchemaTableName(), druidClient.getAllSegmentMetadata(druidTable.getTableName()));
        return tableMetadata.getColumns().stream()
                .collect(toImmutableMap(ColumnMetadata::getName, column -> new DruidColumnHandle(column.getName(), column.getType())));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DruidTableHandle druidTable = (DruidTableHandle) tableHandle;
        DruidColumnHandle druidColumn = (DruidColumnHandle) columnHandle;

        ConnectorTableMetadata tableMetadata = toTableMetadata(druidTable.toSchemaTableName(), druidClient.getAllSegmentMetadata(druidTable.getTableName()));
        return tableMetadata.getColumns().stream()
                .filter(column -> column.getName().equals((druidColumn.getColumnName())))
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
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    // TODO: need to review this impl...
    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, Optional.of(prefix.getSchemaName()));
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    private static ConnectorTableMetadata toTableMetadata(SchemaTableName tableName, List<SegmentAnalysis> segments)
    {
        Map<String, ColumnMetadata> columnMap = new HashMap<>();
        for (SegmentAnalysis segment : segments) {
            Map<String, ColumnMetadata> columns = getColumnMap(segment);
            for (String name : columns.keySet()) {
                columnMap.putIfAbsent(name, columns.get(name));
            }
        }
        return new ConnectorTableMetadata(tableName, ImmutableList.copyOf(columnMap.values()));
    }

    private static Map<String, ColumnMetadata> getColumnMap(SegmentAnalysis segmentAnalysis)
    {
        ImmutableMap.Builder<String, ColumnMetadata> builder = ImmutableMap.builder();
        segmentAnalysis.getColumns().forEach((name, column) -> builder.put(name, toColumnMetadata(name, column)));
        return builder.build();
    }

    private static ColumnMetadata toColumnMetadata(String name, ColumnAnalysis columnAnalysis)
    {
        return new ColumnMetadata(name, getType(name, columnAnalysis.getType()));
    }

    private static Type getType(String name, String type)
    {
        /*
            The default __time column in Druid needs special handling. It's stored natively in LONG type, but should map to TIMESTAMP type in SQL.

                if (name.equals("__time")) {
                    return TIMESTAMP;
                }

            Unfortunately the implementation of TIMESTAMP type in Presto has some comparability issues.
            Therefore, for now just report the native storage type for __time column, which is LONG.

            TODO: fix type for __time column
         */
        switch (type.toUpperCase()) {
            case "STRING":
                return VARCHAR;
            case "LONG":
                return BIGINT;
            case "FLOAT":
                return REAL;
            case "DOUBLE":
                return DOUBLE;
            default:
                throw new IllegalArgumentException("unsupported type: " + type);
        }
    }
}
