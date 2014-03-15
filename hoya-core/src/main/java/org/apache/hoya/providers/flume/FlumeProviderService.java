package org.apache.hoya.providers.flume;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.RoleKeys;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractProviderService;
import org.apache.hoya.providers.ProviderCore;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.servicemonitor.Probe;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.apache.hoya.yarn.service.EventCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeProviderService extends AbstractProviderService implements ProviderCore, HoyaKeys {

    private static final String PROVIDER_NAME = "flume";
    private static final Logger LOGGER = LoggerFactory.getLogger(FlumeProviderService.class);
    private static final ProviderUtils providerUtils = new ProviderUtils(LOGGER);

    public FlumeProviderService() {
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
        Map<String, String> env = HoyaUtils.buildEnvMap(roleOptions);
        env.put("PROPAGATED_CONFDIR", ApplicationConstants.Environment.PWD.$() + "/" + HoyaKeys.PROPAGATED_CONF_DIR_NAME);

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        Map<String, LocalResource> confResources = hoyaFileSystem.submitDirectory(generatedConfPath, HoyaKeys.PROPAGATED_CONF_DIR_NAME);
        localResources.putAll(confResources);

        if (clusterSpec.isImagePathSet()) {
            Path imagePath = new Path(clusterSpec.getImagePath());
            LOGGER.info("using image path {}", imagePath);
            hoyaFileSystem.maybeAddImagePath(localResources, imagePath);
        }
        ctx.setLocalResources(localResources);

        String agentFileName = clusterSpec.getMandatoryOption(FlumeKeys.AGENT_FILE);
        String agentName = clusterSpec.getMandatoryOption(FlumeKeys.AGENT_NAME);
        String port = clusterSpec.getOption(FlumeKeys.PORT, "");

        List<String> command = new ArrayList<String>();
        command.add(buildFlumeScriptBinPath(clusterSpec));
        command.add("agent -n");
        command.add(agentName);
        command.add("-f");
        command.add("$PROPAGATED_CONFDIR/" + agentFileName);
        command.add("--classpath $PROPAGATED_CONFDIR/*.jar");
        command.add("-Xmx" + clusterSpec.getRole(role).get(RoleKeys.JVM_HEAP));
        if (isNotBlank(port)) {
            command.add("-source-port " + port);
        }
        command.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/flume.txt");
        command.add("2>&1");

        ctx.setCommands(Arrays.asList(HoyaUtils.join(command, " ")));
        ctx.setEnvironment(env);
    }

    public String buildFlumeScriptBinPath(ClusterDescription cd) throws FileNotFoundException {
        return providerUtils.buildPathToScript(cd, "bin", FlumeKeys.FLUME_NG_SCRIPT);
    }

    @Override
    public int getDefaultMasterInfoPort() {
        return 0;
    }

    @Override
    public boolean exec(ClusterDescription cd, File confDir, Map<String, String> env,
                        EventCallback execInProgress) throws IOException, HoyaException {
        return false;
    }

    @Override
    public Configuration loadProviderConfigurationInformation(File confDir) throws BadCommandArgumentsException, IOException {
        return new Configuration(false);
    }

    @Override
    public List<Probe> createProbes(ClusterDescription clusterSpec, String url,
                                    Configuration config, int timeout) throws IOException {
        return new ArrayList<Probe>(0);
    }

    @Override
    public List<ProviderRole> getRoles() {
        return FlumeRoles.AGENT_ROLES;
    }

    @Override
    public void validateClusterSpec(ClusterDescription clusterSpec) throws HoyaException {
    }
}
