package org.apache.hoya.providers.flume;

import java.util.Arrays;
import java.util.List;

import org.apache.hoya.providers.ProviderRole;

public class FlumeRoles {

    public static final List<ProviderRole> AGENT_ROLES = Arrays.asList(
            new ProviderRole(FlumeKeys.ROLE_AGENT, 1)
    );

}
