package org.apache.hoya.providers.flume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hoya.providers.ClientProvider;
import org.apache.hoya.providers.HoyaProviderFactory;
import org.apache.hoya.providers.ProviderService;

public class FlumeProviderFactory extends HoyaProviderFactory {

    public FlumeProviderFactory() {
    }

    public FlumeProviderFactory(Configuration configuration) {
        super(configuration);
    }

    @Override
    public ClientProvider createClientProvider() {
        return new FlumeClientProvider(getConf());
    }

    @Override
    public ProviderService createServerProvider() {
        return new FlumeProviderService();
    }
}
