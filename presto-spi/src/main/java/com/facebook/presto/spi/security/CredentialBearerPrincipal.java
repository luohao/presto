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
package com.facebook.presto.spi.security;

import java.security.Principal;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CredentialBearerPrincipal
        implements Principal
{
    private final String name;
    private final Map<String, String> credentials;

    public CredentialBearerPrincipal(String name, Map<String, String> credentials)
    {
        this.name = requireNonNull(name, "name is null");
        this.credentials = requireNonNull(credentials, "requireNonNull is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return name;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CredentialBearerPrincipal that = (CredentialBearerPrincipal) o;

        return Objects.equals(name, that.name) &&
                this.credentials.equals(that.credentials);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this);
    }

    public Optional<String> getCredential(String key)
    {
        return Optional.ofNullable(credentials.get(key));
    }

    public Map<String, String> getAllCredentials()
    {
        return credentials;
    }
}
