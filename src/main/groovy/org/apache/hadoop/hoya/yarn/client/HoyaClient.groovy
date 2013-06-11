/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hoya.yarn.client

import com.beust.jcommander.JCommander
import com.google.common.annotations.VisibleForTesting
import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.hadoop.hoya.HoyaKeys
import org.apache.hadoop.hoya.api.ClusterDescription
import org.apache.hadoop.hoya.api.HoyaAppMasterProtocol
import org.apache.hadoop.hoya.exceptions.BadCommandArgumentsException
import org.apache.hadoop.ipc.RPC
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as FS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.HoyaApp
import org.apache.hadoop.hoya.HoyaExitCodes
import org.apache.hadoop.hoya.exceptions.BadConfigException
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.tools.ConfigHelper
import org.apache.hadoop.hoya.tools.Duration
import org.apache.hadoop.hoya.tools.HoyaUtils
import org.apache.hadoop.hoya.tools.YarnUtils
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.hoya.yarn.ZKIntegration
import org.apache.hadoop.hoya.yarn.appmaster.HoyaMasterServiceArgs
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.client.YarnClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.service.launcher.RunService
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hadoop.yarn.util.Records

import java.nio.ByteBuffer

/**
 * Client service for Hoya
 */
@Commons
@CompileStatic

class HoyaClient extends YarnClientImpl implements RunService, HoyaExitCodes {
  // App master priority
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "default";
  // Amt. of memory resource to request for to run the App Master
  private int amMemory = 10;

  private String[] argv
  private ClientArgs serviceArgs
  public ApplicationId applicationId;

  /**
   * Entry point from the service launcher
   */
  HoyaClient() {
    //any app-wide actions
    new HoyaApp("HoyaClient")
  }

  /**
   * Constructor that takes the command line arguments and parses them
   * via {@link RunService#setArgs(String [])}. That method 
   * MUST NOT be called afterwards.
   * @param args argument list to be treated as both raw and processed
   * arguments.
   */
  public HoyaClient(String...args) {
    setArgs(args)
  }

  @Override //Service
  public String getName() {
    return "Hoya"
  }

  @Override
  public void setArgs(String...args) {
    this.argv = args;
    serviceArgs = new ClientArgs(args)
    serviceArgs.parse()
    serviceArgs.postProcess()
  }

  /**
   * Just before the configuration is set, the args-supplied config is set
   * This is a way to sneak in config changes without subclassing init()
   * (so work with pre/post YARN-117 code)
   * @param conf new configuration.
   */
  @Override
  protected void setConfig(Configuration conf) {
    serviceArgs.applyDefinitions(conf);
    super.setConfig(conf)
  }

/**
   * this is where the work is done.
   * @return the exit code
   * @throws Throwable anything that went wrong
   */
  @Override
  public int runService() throws Throwable {

    //choose the action
    String action = serviceArgs.action
    List<String> actionArgs = serviceArgs.actionArgs
    int exitCode = EXIT_SUCCESS
    String clusterName = serviceArgs.clusterName;
    //actions
    switch(action) {

      case ClientArgs.ACTION_CREATE:
        validateClusterName(clusterName)
        exitCode = createAM(clusterName)
        break;

      case CommonArgs.ACTION_EXISTS:
        validateClusterName(clusterName)
        exitCode = actionExists(clusterName)
        break;

      case ClientArgs.ACTION_HELP:
        log.info("HoyaClient" + serviceArgs.usage())
        break;

      case CommonArgs.ACTION_LIST:
        if (clusterName != null) {
          validateClusterName(clusterName)
        }

        exitCode = actionList(clusterName)
        break;

      case ClientArgs.ACTION_START:
        validateClusterName(clusterName)
        throw new HoyaException("Start: " + actionArgs[0])

      case ClientArgs.ACTION_STATUS:
        validateClusterName(clusterName)
        exitCode = actionStatus(clusterName);
        break;

      case ClientArgs.ACTION_STOP:
        validateClusterName(clusterName)

      default:
        throw new HoyaException(EXIT_UNIMPLEMENTED,
                                "Unimplemented: " + action)
    }

    return exitCode
  }

  protected void validateClusterName(String clustername) {
    if (!HoyaUtils.isClusternameValid(clustername)) {
      throw new BadCommandArgumentsException("Illegal cluster name: `$clustername`")
    }
  }
  
  /**
   * Create the AM
   */
  private int createAM(String clustername) {
    verifyValidClusterSize(serviceArgs.min)
    
    log.info("Setting up application submission context for ASM");
    ApplicationSubmissionContext appContext =
      Records.newRecord(ApplicationSubmissionContext.class);
    GetNewApplicationResponse newApp = super.getNewApplication();
    ApplicationId appId = newApp.applicationId
    // set the application id 
    appContext.applicationId = appId;
    // set the application name
    appContext.applicationName = clustername
    appContext.applicationType = HoyaKeys.APP_TYPE

    //check for debug mode
    if (serviceArgs.debug) {
      appContext.maxAppAttempts = 1
    }
    String username = getUsername()
    String zkPath = ZKIntegration.mkClusterPath(username, clustername)

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer =
      Records.newRecord(ContainerLaunchContext.class);

    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources			
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();


    if (!usingMiniMRCluster) {

      log.info("Copying JARs from local filesystem and add to local environment");
      // Copy the application master jar to the filesystem 
      // Create a local resource to point to the destination jar path 
      String subdir = "";
      String appPath = "$appName/${appId.id}/"
      //add this class
      localResources["hoya.jar"] = submitJarWithClass(this.class, appPath, subdir, "hoya.jar")
      //add lib classes that don't come automatically with YARN AM classpath
      String libdir = "lib/"
      localResources["groovayll.jar"] = submitJarWithClass(GroovyObject.class,
                                                           appPath,
                                                           libdir,
                                                           "groovayll.jar")

      localResources["jcommander.jar"] = submitJarWithClass(JCommander.class,
                                                            appPath,
                                                            libdir,
                                                            "jcommander.jar")
      localResources["ant.jar"] = submitJarWithClass(JCommander.class,
                                                            appPath,
                                                            libdir,
                                                            "ant.jar")
      String appRoot = "/yarnapps/$appName/${appId.id}/"
      String zookeeperRoot = appRoot
      if (serviceArgs.hbasezkpath == null) {
       zookeeperRoot = "/yarnapps/$appName/${appId.id}/"
      }
      URI hdfsRootDir
      if (serviceArgs.filesystemURL != null) {
        hdfsRootDir = new URI(serviceArgs.filesystemURL.getScheme(), serviceArgs.filesystemURL.getAuthority(),
                appRoot); //TODO: error checks
      } else {
        hdfsRootDir = new URI("hdfs://" + appRoot);
      }
      Path generatedConfDir = new Path("/tmp");
      if (serviceArgs.generatedConfdir != null) {
        generatedConfDir = serviceArgs.generatedConfdir;
      }
      String subDirName = appName + "-" + username + "/" + "${appId.id}";
      Configuration config = ConfigHelper.generateConfig(
          [
              "hdfs.rootdir": hdfsRootDir.toString(),
              "zookeeper.znode.parent": zookeeperRoot
          ],
          subDirName,
          generatedConfDir);
      // Send the above generated config file to Yarn. This will be the config
      // for HBase
    }

    // Set the log4j properties if needed 
/*
    if (!log4jPropFile.isEmpty()) {
      Path log4jSrc = new Path(log4jPropFile);
      Path log4jDst = new Path(fs.getHomeDirectory(), "log4j.props");
      fs.copyFromLocalFile(false, true, log4jSrc, log4jDst);
      FileStatus log4jFileStatus = fs.getFileStatus(log4jDst);
      LocalResource log4jRsrc = Records.newRecord(LocalResource.class);
      log4jRsrc.setType(LocalResourceType.FILE);
      log4jRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
      log4jRsrc.setResource(ConverterUtils.getYarnUrlFromURI(log4jDst.toUri()));
      log4jRsrc.setTimestamp(log4jFileStatus.getModificationTime());
      log4jRsrc.setSize(log4jFileStatus.getLen());
      localResources.put("log4j.properties", log4jRsrc);
    }

*/
    // The shell script has to be made available on the final container(s)
    // where it will be executed. 
    // To do this, we need to first copy into the filesystem that is visible 
    // to the yarn framework. 
    // We do not need to set this as a local resource for the application 
    // master as the application master does not need it. 		
/*
    String hdfsShellScriptLocation = "";
    long hdfsShellScriptLen = 0;
    long hdfsShellScriptTimestamp = 0;
    if (!shellScriptPath.isEmpty()) {
      Path shellSrc = new Path(shellScriptPath);
      String shellPathSuffix = appName + "/" + appId.getId() + "/ExecShellScript.sh";
      Path shellDst = new Path(fs.getHomeDirectory(), shellPathSuffix);
      fs.copyFromLocalFile(false, true, shellSrc, shellDst);
      hdfsShellScriptLocation = shellDst.toUri().toString();
      FileStatus shellFileStatus = fs.getFileStatus(shellDst);
      hdfsShellScriptLen = shellFileStatus.getLen();
      hdfsShellScriptTimestamp = shellFileStatus.getModificationTime();
    }
*/

    // Set local resource info into app master container launch context
    amContainer.localResources = localResources;
    def env = [:]

    env['CLASSPATH'] = buildClasspath()

    amContainer.environment = env;

    String rmAddr = NetUtils.getHostPortString(YarnUtils.getRmSchedulerAddress(config))

    //build up the args list, intially as anyting
    List commands = []
    commands << ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java"
    //insert any JVM options
    commands << HoyaKeys.JAVA_FORCE_IPV4;
    //add the generic sevice entry point
    commands << ServiceLauncher.ENTRY_POINT
    //immeiately followed by the classname
    commands << HoyaMasterServiceArgs.CLASSNAME
    //now the app specific args
    commands << HoyaMasterServiceArgs.ARG_DEBUG
    commands << HoyaMasterServiceArgs.ACTION_CREATE
    commands << clustername
    commands << HoyaMasterServiceArgs.ARG_MIN
    commands << (Integer)serviceArgs.min
    commands << HoyaMasterServiceArgs.ARG_MAX
    commands << (Integer)serviceArgs.max
    
    //spec out the RM address
    commands << HoyaMasterServiceArgs.ARG_RM_ADDR;
    commands << rmAddr;
        
    //zk details -HBASE needs fs.default.name
    
    //hbase needs path inside ZK; skip ZK connect
    // use env variables & have that picked up and template it. ${env.SYZ}
    if (serviceArgs.zookeeper) {
      commands << HoyaMasterServiceArgs.ARG_ZOOKEEPER
      commands << serviceArgs.zookeeper
    }
    if (serviceArgs.hbasehome) {
      //HBase home
      commands << HoyaMasterServiceArgs.ARG_HBASE_HOME
      commands << serviceArgs.hbasehome
    }
    if (serviceArgs.hbasezkpath) {
      //HBase ZK path
      commands << HoyaMasterServiceArgs.ARG_HBASE_ZKPATH
      commands << serviceArgs.hbasezkpath
    }
    if (serviceArgs.hbaseCommand) {
      //explicit hbase command set
      commands << CommonArgs.ARG_X_HBASE_COMMAND 
      commands << serviceArgs.hbaseCommand
    }
    if (serviceArgs.xTest) {
      //test flag set
      commands << CommonArgs.ARG_X_TEST 
    }
    if (serviceArgs.xNoMaster) {
      //server is not to create the master, just come up.
      //purely for test purposes
      commands << CommonArgs.ARG_X_NO_MASTER 
    }
  
    commands << HoyaMasterServiceArgs.ARG_FILESYSTEM
    commands << config.get(FS.FS_DEFAULT_NAME_KEY);

    //path in FS can be unqualified
    commands << HoyaMasterServiceArgs.ARG_PATH
    commands << "services/hoya/"
    commands << "1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/out.txt";
    commands << "2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/err.txt";

    String cmdStr = commands.join(" ")
    log.info("Completed setting up app master command $cmdStr");
    //sanity check: no null entries are allowed
    commands.each { assert it !=null }
    //uses the star-dot operator to apply the tostring method to all elements
    //of the array, returnigna new array
    List<String> commandListStr = commands*.toString();
    
    amContainer.commands = commandListStr
    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.memory = amMemory;
    appContext.resource = capability;
    Map<String, ByteBuffer> serviceData = [:]
    // Service data is a binary blob that can be passed to the application
    // Not needed in this scenario
    amContainer.serviceData = serviceData;

    // The following are not required for launching an application master 
    // amContainer.setContainerId(containerId);

    appContext.AMContainerSpec = amContainer;

    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide? 
    pri.priority = amPriority;
    appContext.priority = pri;

    // Set the queue to which this application is to be submitted in the RM
    appContext.queue = amQueue;

    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success 
    // or an exception thrown to denote some form of a failure
    log.info("Submitting application to ASM");

    //submit the application
    applicationId = submitApplication(appContext)

    int exitCode
    //wait for the submit state to be reached
    ApplicationReport report = monitorAppToState(new Duration(60000),
                                                 YarnApplicationState.ACCEPTED);
    
    //may have failed, so check that
    if (YarnUtils.hasAppFinished(report)) {
      exitCode = buildExitCode(appId, report)
    } else {
      //exit unless there is a wait
      exitCode = EXIT_SUCCESS

      if (serviceArgs.waittime != 0) {
        //waiting for state to change
        Duration duration = new Duration(serviceArgs.waittime)
        duration.start()
        report = monitorAppToState(duration,
                                   YarnApplicationState.RUNNING);
        if (report && report.yarnApplicationState == YarnApplicationState.RUNNING) {
          exitCode = EXIT_SUCCESS
        } else {
          killRunningApplication(appId);
          exitCode = buildExitCode(appId, report)
        }
      }
    }
    return exitCode
  }


  public String getUsername() {
    return UserGroupInformation.getCurrentUser().getShortUserName();
  }

  private LocalResource submitJarWithClass(Class clazz, String appPath, String subdir, String jarName) {
    File localFile = HoyaUtils.findContainingJar(clazz);
    if (!localFile) {
      throw new HoyaException("Could not find JAR containing "
                                                 + clazz);
    }
    LocalResource resource = submitFile(localFile, appPath, subdir, jarName)
    resource
  }

  private LocalResource submitFile(File localFile, String appPath, String subdir, String destFileName) {
    FS hdfs = clusterFS;
    Path src = new Path(localFile.toString());
    String pathSuffix = appPath + "${subdir}$destFileName";
    Path destPath = new Path(hdfs.homeDirectory, pathSuffix);

    hdfs.copyFromLocalFile(false, true, src, destPath);

    // Set the type of resource - file or archive
    // archives are untarred at destination
    // we don't need the jar file to be untarred for now
    LocalResource resource = YarnUtils.createAmResource(hdfs,
                                                        destPath,
                                                        LocalResourceType.FILE)
    return resource
  }

  /**
   * Create an AM resource from the 
   * @param hdfs HDFS or other filesystem in use
   * @param destPath dest path in filesystem
   * @param resourceType resource type
   * @return the resource set up wih application-level visibility and the
   * timestamp & size set from the file stats.
   */
  
  /**
   * Get the filesystem of this cluster
   * @return the FS of the config
   */
  private FS getClusterFS() {
    FS.get(config)
  }

  /**
   * Verify that there are enough nodes in the cluster
   * @param requiredNumber required # of nodes
   * @throws BadConfigException if the config is wrong
   */
  private void verifyValidClusterSize(int requiredNumber) {
    if (requiredNumber==0) {
      return
    }
    int nodeManagers = yarnClusterMetrics.numNodeManagers
    if (nodeManagers < requiredNumber) {
      throw new BadConfigException("Not enough nodes in the cluster:" +
                                         " need $requiredNumber" +
                                         " -but there are only $nodeManagers nodes");
    }
  }

  private String buildClasspath() {
// Add AppMaster.jar location to classpath
    // At some point we should not be required to add 
    // the hadoop specific classpaths to the env. 
    // It should be provided out of the box. 
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder()
    // add the runtime classpath needed for tests to work
    if (getUsingMiniMRCluster()) {
      //for mini cluster we pass down the java CP properties
      //and nothing else
      classPathEnv.append(System.getProperty("java.class.path"));
    } else {
      classPathEnv.append(ApplicationConstants.Environment.CLASSPATH.$())
          .append(File.pathSeparatorChar).append("./*");
      for (String c : config.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        classPathEnv.append(File.pathSeparatorChar);
        classPathEnv.append(c.trim());
      }
      classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");
    }
    return classPathEnv.toString()
  }

  private boolean getUsingMiniMRCluster() {
    return config.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)
  }

  private String getAppName() {
    "hoya"
  }

/**
 * Monitor the submitted application for reaching the requested state.
 * Will also report if the app reaches a later state (failed, killed, etc)
 * Kill application if duration!= null & time expires. 
 * @param appId Application Id of application to be monitored
 * @param duration how long to wait
 * @param desiredState desired state.
 * @return true if application completed successfully
 * @throws YarnException YARN or app issues
 * @throws IOException IO problems
 */
  @VisibleForTesting
  public int monitorAppToCompletion(Duration duration)
      throws YarnException, IOException {


    ApplicationReport report = monitorAppToState(duration,
                                       YarnApplicationState.FINISHED)

    return buildExitCode(applicationId, report)
  }

  /**
   * Wait for the app to start running (or go past that state)
   * @param duration time to wait
   * @return the app report; null if the duration turned out
   * @throws YarnException YARN or app issues
   * @throws IOException IO problems
   */
  @VisibleForTesting
  public ApplicationReport monitorAppToRunning(Duration duration)
      throws YarnException, IOException {
    return monitorAppToState(duration,
                             YarnApplicationState.RUNNING)

  }

  private boolean maybeKillApp(ApplicationReport report) {
    if (!report) {
      log.debug("Reached client specified timeout for application. Killing application");
      forceKillApplication();
    }
    return false;
  }
  /**
   * Build an exit code for an application Id and its report.
   * If the report parameter is null, the app is killed
   * @param appId app
   * @param report report
   * @return the exit code
   */
  private int buildExitCode(ApplicationId appId, ApplicationReport report) {
    if (!report) {
      log.info("Reached client specified timeout for application. Killing application");
      forceKillApplication();
      return EXIT_TIMED_OUT;
    }

    YarnApplicationState state = report.yarnApplicationState
    FinalApplicationStatus dsStatus = report.finalApplicationStatus;
    switch (state) {
      case YarnApplicationState.FINISHED:
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          log.info("Application has completed successfully");
          return EXIT_SUCCESS;
        } else {
          log.info("Application finished unsuccessfully." +
                   " YarnState=" + state + ", DSFinalStatus=" + dsStatus +
                   ". Breaking monitoring loop");
          return EXIT_YARN_SERVICE_FINISHED_WITH_ERROR;
        }

      case YarnApplicationState.KILLED:
        log.info("Application did not finish. YarnState=$state, DSFinalStatus=$dsStatus");
        return EXIT_YARN_SERVICE_KILLED;

      case YarnApplicationState.FAILED:
        log.info("Application Failed. YarnState=$state, DSFinalStatus=$dsStatus");
        return EXIT_YARN_SERVICE_FAILED;
      default:
        //not in any of these states
        return EXIT_SUCCESS;
    }
  }
/**
 * Monitor the submitted application for reaching the requested state.
 * Will also report if the app reaches a later state (failed, killed, etc)
 * Kill application if duration!= null & time expires. 
 * @param appId Application Id of application to be monitored
 * @param duration how long to wait -must be more than 0
 * @param desiredState desired state.
 * @return the application report -null on a timeout
 * @throws YarnException
 * @throws IOException
 */
  @VisibleForTesting
  public ApplicationReport monitorAppToState(
      Duration duration, YarnApplicationState desiredState)
  throws YarnException, IOException {

    duration.start();
    if (duration.limit <= 0) {
      throw new HoyaException("Invalid duration of monitoring");
    }
    while (true) {


      // Get application report for the appId we are interested in 
      ApplicationReport report = getApplicationReport(applicationId);

      log.info("Got application report from ASM for, appId=${applicationId}, clientToken=${report.clientToken}, appDiagnostics=${report.diagnostics}, appMasterHost=${report.host}, appQueue=${report.queue}, appMasterRpcPort=${report.rpcPort}, appStartTime=${report.startTime}, yarnAppState=${report.yarnApplicationState}, distributedFinalState=${report.finalApplicationStatus}, appTrackingUrl=${report.trackingUrl}, appUser=${report.user}");

      YarnApplicationState state = report.yarnApplicationState;
      if (state >= desiredState) {
        log.debug("App in desired state (or higher) : $state")
        return report;
      }
      if (duration.limitExceeded) {
        return null;
      }

      // sleep 1s.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
        log.debug("Thread sleep in monitoring loop interrupted");
      }
    }
  }

  /**
   * Kill the submitted application by sending a call to the ASM
   * @throws YarnException
   * @throws IOException
   */
  public boolean forceKillApplication()
        throws YarnException, IOException {
    if (applicationId != null) {
      killRunningApplication(applicationId);
      return true;
    }
    return false;
  }

  /**
   * Kill a running application
   * @param applicationId
   * @return the response
   * @throws YarnException YARN problems
   * @throws IOException IO problems
   */
  private KillApplicationResponse killRunningApplication(ApplicationId applicationId) throws
      YarnException,
      IOException {
    log.info("Killing application " + applicationId);
    KillApplicationRequest request =
      Records.newRecord(KillApplicationRequest.class);
    request.setApplicationId(applicationId);
    return rmClient.forceKillApplication(request);
  }

  /**
   * List Hoya instances belonging to a specific user
   * @param user user: "" means all users
   * @return a possibly empty list of Hoya AMs
   */
  @VisibleForTesting
  public List<ApplicationReport> listHoyaInstances(String user)
    throws YarnException, IOException {
    List<ApplicationReport> allApps = applicationList;
    List<ApplicationReport> results = []
    allApps.each { ApplicationReport report ->
      if (   report.applicationType == HoyaKeys.APP_TYPE
          && (!user || user == report.user)) {
        results << report;
      }
    }
    return results;
  }

  /**
   * Implement the list action: list all nodes
   * @return exit code of 0 if a list was created
   */
  @VisibleForTesting
  public int actionList(String clustername) {
    String user = serviceArgs.user
    List<ApplicationReport> instances = listHoyaInstances(user);

    if (!clustername) {
      log.info("Hoya instances for ${user ? user : 'all users'} : ${instances.size()} ");
      instances.each { ApplicationReport report ->
        logAppReport(report)
      }
      return EXIT_SUCCESS;
    } else {
      log.debug("Listing cluster named $clustername")
      ApplicationReport report = findClusterInInstanceList(instances, clustername)
      if (report) {
        logAppReport(report)
        return EXIT_SUCCESS;
      } else {
        throw unknownClusterException(clustername)
      }
    }
  }

  public void logAppReport(ApplicationReport report) {
    log.info("Name        : ${report.name}")
    log.info("YARN status : ${report.yarnApplicationState}")
    log.info("Start Time  : ${report.startTime}")
    log.info("Finish Time : ${report.startTime}")
    log.info("RPC         : ${report.host}:${report.rpcPort}")
    log.info("Diagnostics : ${report.diagnostics}")
  }

  /**
   * Implement the islive action: probe for a cluster of the given name existing
   * 
   * @return exit code
   */
  @VisibleForTesting
  public int actionExists(String name) {
    ApplicationReport instance = findInstance(getUsername(), name)
    if (!instance) {
      throw unknownClusterException(name)
    }
    return EXIT_SUCCESS;
  }

  @VisibleForTesting
  public ApplicationReport findInstance(String user, String appname) {
    log.debug("Looking for instances of user $user")
    List<ApplicationReport> instances = listHoyaInstances(user);
    log.debug("Found $instances of user $user")
    return findClusterInInstanceList(instances, appname)
  }

  public ApplicationReport findClusterInInstanceList(List<ApplicationReport> instances, String appname) {
    ApplicationReport found = null;
    instances.each { ApplicationReport report ->
      log.debug("Report named ${report.name}")
      if (report.name == appname) {
        found = report;
      }
    }
    return found;
  }

  @VisibleForTesting
  public HoyaAppMasterProtocol connect(ApplicationReport report) {
    String host = report.host
    int port = report.rpcPort
    String address= report.host + ":" + port;
    if (!host || !port ) {
      throw new HoyaException(EXIT_CONNECTIVTY_PROBLEM,
                              "Hoya instance $report.name isn't" +
                              " providing a valid address for the" +
                              " Hoya RPC protocol: <$address>")
    }
    InetSocketAddress addr = NetUtils.createSocketAddrForHost(host, port);
    log.debug("Connecting to Hoya Server at " + addr);
    def protoProxy = RPC.getProtocolProxy(HoyaAppMasterProtocol,
                        HoyaAppMasterProtocol.versionID,
                        addr,
                        UserGroupInformation.getCurrentUser(),
                        getConfig(),
                        NetUtils.getDefaultSocketFactory(getConfig()),
                        15000,
                        null)
    HoyaAppMasterProtocol hoyaServer = protoProxy.proxy
    log.debug("Connected to Hoya Server at " + addr);
    return hoyaServer;
  }
  /**
   * Status operation; 'name' arg defines cluster name.
   * @return
   */
  @VisibleForTesting
  public int actionStatus(String clustername) {
    ClusterDescription status = getClusterStatus(clustername)
    log.info(status.toJsonString());
    return EXIT_SUCCESS
  }

  @VisibleForTesting
  public ClusterDescription getClusterStatus(String clustername) {
    ApplicationReport instance = findInstance(getUsername(), clustername)
    if (!instance) {
      throw unknownClusterException(clustername)
    }
    HoyaAppMasterProtocol appMaster = connect(instance);
    String statusJson = appMaster.getClusterStatus()
    log.info(statusJson)
    ClusterDescription cd = ClusterDescription.fromJson(statusJson)
    return cd
  }

  public HoyaException unknownClusterException(String clustername) {
    return new HoyaException(EXIT_UNKNOWN_HOYA_CLUSTER,
                            "Hoya cluster not found: '${clustername}' ")
  }

}