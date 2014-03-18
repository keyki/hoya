package org.apache.hoya.providers.tomcat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractProviderService;
import org.apache.hoya.providers.ProviderCore;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.servicemonitor.Probe;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.yarn.service.EventCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TomcatProviderService extends AbstractProviderService implements ProviderCore, HoyaKeys {

    private static final String PROVIDER_NAME = "tomcat";
    private static final Logger LOGGER = LoggerFactory.getLogger(TomcatProviderService.class);
    private static final ProviderUtils providerUtils = new ProviderUtils(LOGGER);

    public TomcatProviderService() {
        super(PROVIDER_NAME);
    }

    @Override
    public void buildContainerLaunchContext(ContainerLaunchContext ctx,
            Container container,
            String role,
            HoyaFileSystem hoyaFileSystem,
            Path generatedConfPath,
            ClusterDescription clusterSpec,
            Map<String, String> roleOptions,
            Path containerTmpDirPath) throws IOException, HoyaException {
        //TODO implement DR
    }

    @Override
    public int getDefaultMasterInfoPort() {
        return 0;
    }

    @Override
    public boolean exec(ClusterDescription cd, File confDir, Map<String, String> env, EventCallback execInProgress) throws IOException, HoyaException {
        return false;
    }

    @Override
    public Configuration loadProviderConfigurationInformation(File confDir) throws BadCommandArgumentsException, IOException {
        return new Configuration(false);
    }

    @Override
    public List<Probe> createProbes(ClusterDescription clusterSpec, String url, Configuration config, int timeout) throws IOException {
        return new ArrayList<Probe>(0);
    }

    @Override
    public List<ProviderRole> getRoles() {
        return TomcatRoles.SERVER_ROLES;
    }

    @Override
    public void validateClusterSpec(ClusterDescription clusterSpec) throws HoyaException {

    }
}
