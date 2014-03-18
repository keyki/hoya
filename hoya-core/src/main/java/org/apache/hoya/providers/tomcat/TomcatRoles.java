package org.apache.hoya.providers.tomcat;

import java.util.Arrays;
import java.util.List;

import org.apache.hoya.providers.ProviderRole;

public class TomcatRoles {

    public static final List<ProviderRole> SERVER_ROLES = Arrays.asList(
            new ProviderRole(TomcatKeys.ROLE_SERVER, 1)
    );

}
