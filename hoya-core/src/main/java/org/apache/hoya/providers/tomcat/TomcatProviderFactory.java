package org.apache.hoya.providers.tomcat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hoya.providers.ClientProvider;
import org.apache.hoya.providers.HoyaProviderFactory;
import org.apache.hoya.providers.ProviderService;

public class TomcatProviderFactory extends HoyaProviderFactory {

    public TomcatProviderFactory() {
    }

    public TomcatProviderFactory(Configuration configuration) {
        super(configuration);
    }

    @Override
    public ClientProvider createClientProvider() {
        return new TomcatClientProvider(getConf());
    }

    @Override
    public ProviderService createServerProvider() {
        return new TomcatProviderService();
    }

}
