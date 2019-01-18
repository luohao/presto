package com.facebook.presto.plugin.bigquery;

import com.facebook.presto.plugin.jdbc.JdbcPlugin;

public class BigQueryPlugin
        extends JdbcPlugin
{
    public BigQueryPlugin()
    {
        super("bigquery", new BigQueryClientModule());
    }
}
