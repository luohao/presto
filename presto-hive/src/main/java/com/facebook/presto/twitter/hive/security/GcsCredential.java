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

import static java.util.Objects.requireNonNull;

public class GcsCredential
{
    public static final String GCS_ACCESS_TOKEN = "presto_gcs_access_token";
    private final String tokenValue;

    public GcsCredential(String tokenValue)
    {
        this.tokenValue = requireNonNull(tokenValue, "token is null");
    }

    public String getTokenString()
    {
        return tokenValue;
    }

    public static GcsCredential createGcsCredential(String tokenValue)
    {
        if (tokenValue == null) {
            return null;
        }
        return new GcsCredential(tokenValue);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("GcsCredential{");
        sb.append("tokenValue='").append(tokenValue).append('\'');
        sb.append('}');
        return sb.toString();
    }}
