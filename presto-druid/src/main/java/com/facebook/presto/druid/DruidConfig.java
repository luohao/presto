package com.facebook.presto.druid;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class DruidConfig
{
    private String druidCoordinatorUrl;

    @NotNull
    public String getDruidCoordinatorUrl()
    {
        return druidCoordinatorUrl;
    }

    @Config("druid-coordinator-url")
    public DruidConfig setDruidCoordinatorUrl(String druidCoordinatorUrl)
    {
        this.druidCoordinatorUrl = druidCoordinatorUrl;
        return this;
    }
}
