package org.apache.hoya.providers.flume;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractProviderCore;
import org.apache.hoya.providers.ClientProvider;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.tools.ConfigHelper;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeClientProvider extends AbstractProviderCore implements ClientProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlumeClientProvider.class);

    public FlumeClientProvider(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Map<String, String> createDefaultClusterRole(String roleName) throws HoyaException, IOException {
        Map<String, String> roleMap = new HashMap<String, String>();
        if (roleName.equals(FlumeKeys.ROLE_AGENT)) {
            Configuration conf = ConfigHelper.loadMandatoryResource("org/apache/hoya/providers/flume/role-flume-master.xml");
            HoyaUtils.mergeEntries(roleMap, conf);
        }
        return roleMap;
    }

    @Override
    public Map<String, LocalResource> prepareAMAndConfigForLaunch(HoyaFileSystem hoyaFileSystem, Configuration serviceConf, ClusterDescription clusterSpec,
                                                                  Path originConfDirPath, Path generatedConfDirPath, Configuration clientConfExtras,
                                                                  String libdir, Path tempPath) throws IOException, HoyaException {
        Configuration configuration = new Configuration(false);
        ConfigHelper.saveConfig(serviceConf, configuration, generatedConfDirPath, FlumeKeys.FLUME_CONFIG);
        return hoyaFileSystem.submitDirectory(generatedConfDirPath, HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    }

    @Override
    public Configuration getDefaultClusterConfiguration() throws FileNotFoundException {
        return new Configuration(false);
    }

    @Override
    public void prepareAMResourceRequirements(ClusterDescription clusterSpec, Resource capability) {
    }

    @Override
    public void prepareAMServiceData(ClusterDescription clusterSpec, Map<String, ByteBuffer> serviceData) {
    }

    @Override
    public void reviewAndUpdateClusterSpec(ClusterDescription clusterSpec) throws HoyaException {
    }

    @Override
    public void preflightValidateClusterConfiguration(HoyaFileSystem hoyaFileSystem, String clustername, Configuration configuration, ClusterDescription clusterSpec, Path clusterDirPath, Path generatedConfDirPath, boolean secure) throws HoyaException, IOException {
    }

    @Override
    public Configuration create(Configuration conf) {
        return conf;
    }

    @Override
    public String getName() {
        return FlumeKeys.PROVIDER_NAME;
    }

    @Override
    public List<ProviderRole> getRoles() {
        return FlumeRoles.AGENT_ROLES;
    }
}
