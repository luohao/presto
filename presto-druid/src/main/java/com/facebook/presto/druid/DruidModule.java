package com.facebook.presto.druid;

import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class DruidModule
        implements Module
{
    private final String catalogName;
    private final TypeManager typeManager;

    public DruidModule(String catalogName, TypeManager typeManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalog name is null");
        this.typeManager = requireNonNull(typeManager, "type manager name is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(DruidConnector.class).in(Scopes.SINGLETON);
        binder.bind(DruidMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DruidClient.class).in(Scopes.SINGLETON);
        binder.bind(DruidSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DruidRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DruidConfig.class);    }
}
