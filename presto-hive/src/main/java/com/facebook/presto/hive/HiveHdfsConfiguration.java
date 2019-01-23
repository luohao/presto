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
package com.facebook.presto.hive;

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.spi.security.CredentialBearerPrincipal;
import com.facebook.presto.twitter.hive.security.GcsConfigurationUpdater;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;

import static com.facebook.presto.hive.util.ConfigurationUtils.copy;
import static com.facebook.presto.hive.util.ConfigurationUtils.getInitialConfiguration;
import static com.facebook.presto.twitter.hive.security.DirectTokenProvider.GCS_ACCESS_KEY_CONF;
import static java.util.Objects.requireNonNull;

public class HiveHdfsConfiguration
        implements HdfsConfiguration
{
    private static final Configuration INITIAL_CONFIGURATION = getInitialConfiguration();

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<Configuration> hadoopConfiguration = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            Configuration configuration = new Configuration(false);
            copy(INITIAL_CONFIGURATION, configuration);
            updater.updateConfiguration(configuration);
            return configuration;
        }
    };

    private final HdfsConfigurationUpdater updater;

    @Inject
    public HiveHdfsConfiguration(HdfsConfigurationUpdater updater)
    {
        this.updater = requireNonNull(updater, "updater is null");
    }

    @Override
    public Configuration getConfiguration(HdfsContext context, URI uri)
    {
        Configuration config = new Configuration();
        copy(hadoopConfiguration.get(), config);
        context.getIdentity().getPrincipal().ifPresent(x -> {
            if (x instanceof CredentialBearerPrincipal) {
                ((CredentialBearerPrincipal) x).getCredential(GCS_ACCESS_KEY_CONF)
                        .ifPresent(t -> GcsConfigurationUpdater.updateConfiguration(config, t));
            }
        });
        return config;
    }
}
