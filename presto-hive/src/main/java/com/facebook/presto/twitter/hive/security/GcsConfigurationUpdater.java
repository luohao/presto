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
package com.facebook.presto.twitter.hive.security;

import org.apache.hadoop.conf.Configuration;

import static com.facebook.presto.twitter.hive.security.DirectTokenProvider.GCS_ACCESS_KEY_CONF;

public class GcsConfigurationUpdater
{
    private GcsConfigurationUpdater() {}

    // FIXME: as of now, GCS connector still doens't allow token for service account
    public static void updateConfiguration(Configuration config, String token)
    {
        config.set("fs.gs.auth.access.token.provider.impl", DirectTokenProvider.class.getName());
        config.set(GCS_ACCESS_KEY_CONF, token);
    }
}
