package com.facebook.presto.plugin.bigquery;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;

import javax.inject.Inject;

public class BigQueryClient
        extends BaseJdbcClient
{
    @Inject
    public BigQueryClient(JdbcConnectorId connectorId, BaseJdbcConfig config, String identifierQuote, ConnectionFactory connectionFactory)
    {
        super(connectorId, config, identifierQuote, connectionFactory);
    }
}
