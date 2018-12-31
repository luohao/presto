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
package com.facebook.presto.password;

import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.CredentialBearerPrincipal;
import com.facebook.presto.spi.security.PasswordAuthenticator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.security.Principal;
import java.util.Map;

public class ConnectorCredentialAuthenticator
        implements PasswordAuthenticator
{
    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        // the password is a json doc containing a map of <credential-name, credential> pairs
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> credentials;
        try {
            credentials = objectMapper.readValue(password, Map.class);
        }
        catch (IOException e) {
            throw new AccessDeniedException("Invalid credentials");
        }

        return new CredentialBearerPrincipal(user, credentials);
    }
}
