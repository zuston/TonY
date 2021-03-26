/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.UTCClock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.linkedin.tony.events.ApplicationFinished;
import com.linkedin.tony.events.ApplicationInited;
import com.linkedin.tony.events.Event;
import com.linkedin.tony.events.EventHandler;
import com.linkedin.tony.events.EventType;
import com.linkedin.tony.events.TaskFinished;
import com.linkedin.tony.events.TaskStarted;
import com.linkedin.tony.models.JobMetadata;
import com.linkedin.tony.plugin.TFEvalMetricCollector;
import com.linkedin.tony.plugin.TFTrainMetricCollector;
import com.linkedin.tony.rpc.ApplicationRpc;
import com.linkedin.tony.rpc.ApplicationRpcServer;
import com.linkedin.tony.rpc.MetricsRpc;
import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.rpc.impl.MetricsRpcServer;
import com.linkedin.tony.rpc.impl.TaskStatus;
import com.linkedin.tony.tensorflow.TensorFlowContainerRequest;
import com.linkedin.tony.tensorflow.TensorFlowRedirectServer;
import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.tensorflow.TonySession.TonyTask;
import com.linkedin.tony.util.OpalUtils;
import com.linkedin.tony.util.Utils;

import static com.linkedin.tony.TonyConfigurationKeys.AM_PLUGIN_TASKS;
import static com.linkedin.tony.TonyConfigurationKeys.AM_PLUGIN_TASKS_ENABLED;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_AM_PLUGIN_TASKS_ENABLED;
import static com.linkedin.tony.TonyConfigurationKeys.DEFAULT_APPLICATION_DISTRIBUTED_MODE;


public class ApplicationMaster {
  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  /**
   * Metadata + History Server related variables
   */
  private String appIdString;
  private FileSystem resourceFs; // FileSystem used to access resources for the job, like jars and zips
  private FileSystem historyFs;  // FileSystem used to write history-related files like config and events.
                                 // In some HDFS setups, operators may wish to write history files to a different
                                 // NameNode instance than data and other files.
  private String tonyHistoryFolder;
  private Path jobDir = null;
  private String user = null;
  private BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
  private TonyConfigurationKeys.DistributedMode distributedMode;

  // Container info
  private int amRetryCount;
  private long workerTimeout;
  private String hdfsClasspath;
  private int amPort;
  private ByteBuffer allTokens;
  private Map<String, LocalResource> localResources = new ConcurrentHashMap<>();
  private Configuration tonyConf = new Configuration(false);
  private ContainerId containerId;
  private TensorFlowRedirectServer redirectServer;

  /** The environment set up for the TaskExecutor **/
  private Map<String, String> containerEnv = new ConcurrentHashMap<>();

  /** The environment passed from users to the training job. Note this is very different from the above. **/
  private Map<String, String> shellEnv = new HashMap<>();

  /** Map of session to containers */
  private Map<Integer, List<Container>> sessionContainersMap = new ConcurrentHashMap<>();

  /** Node manager delegate **/
  private NMClientAsync nmClientAsync;
  /** Resource manager **/
  private AMRMClientAsync<ContainerRequest> amRMClient;

  private Map<String, List<ContainerRequest>> jobTypeToContainerRequestsMap = new HashMap<>();

  /** Tony session **/
  private TonySession session = new TonySession(); // Create a dummy session for single node training.
  private TonySession.Builder sessionBuilder;

  /** Configuration **/
  private Configuration yarnConf;
  private Configuration hdfsConf;

  /** Cluster spec **/
  private ApplicationRpcServer applicationRpcServer;

  /** Set to false when testing locally / running in insecure cluster **/
  private boolean secureMode;

  /** Single node training **/
  private boolean singleNode;
  private boolean preprocessFinished = false;
  private int preprocessExitCode = 0;
  private String proxyUrl;

  /** Preprocessing job **/
  private boolean enablePreprocessing = false;

  /** Lifecycle control **/
  private long appTimeout;

  /** We use this to give the client a chance to get task updates before the AM shuts down. */
  private volatile boolean clientSignalToStop = false; // client signal to stop

  /** Metrics and events **/
  private MetricsRpcServer metricsRpcServer;
  private EventHandler eventHandler;

  /** HeartBeat monitor **/
  private final AbstractLivelinessMonitor<TonyTask> hbMonitor;
  private int hbInterval;
  private int maxConsecutiveHBMiss;
  private volatile boolean taskHasMissesHB = false;
  private Thread mainThread;

  /** report metrics to opal **/
  private MetricsReporter metricsReporter;
  private final ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(1);

  /**
   * how to support tensorflow metric collect?
   * 1. enabled TonY plugin tasks by config
   * 2. put plugin tasks into executor thread pool
   */
  private final ExecutorService pluginExecutorsThreadPool = Executors.newCachedThreadPool();

  private long registrationTimeoutMs = tonyConf.getInt(TonyConfigurationKeys.CONTAINER_ALLOCATION_TIMEOUT,
          TonyConfigurationKeys.DEFAULT_CONTAINER_ALLOCATION_TIMEOUT);

  private ApplicationMaster() {
    hdfsConf = new Configuration(false);
    yarnConf = new Configuration(false);

    hbMonitor = new AbstractLivelinessMonitor<TonyTask>("Tony Task liveliness Monitor", new UTCClock()) {
      @Override
      protected void expire(TonyTask task) {
        onTaskDeemedDead(task);
      }

      @Override
      protected void serviceStart() throws Exception {
        // setMonitorInterval(int) changed to setMonitorInterval(long) in Hadoop 2.9,
        // so to support both cases, we use reflection
        int monitorInterval = hbInterval * 3;
        for (Method m : this.getClass().getDeclaredMethods()) {
          if (m.getName().equals(Constants.SET_MONITOR_INTERVAL_METHOD)) {
            m.invoke(this, monitorInterval);
            break;
          }
        }
        setExpireInterval(hbInterval * Math.max(3, maxConsecutiveHBMiss)); // Be at least == monitoring interval
        super.serviceStart();
      }
    };
  }

  /**
   * Parse command line options and initialize ApplicationMaster
   * @return whether the initialization is successful or not.
   */
  private boolean init(String[] args) {
    tonyConf.addResource(new Path(Constants.TONY_FINAL_XML));

    Utils.initYarnConf(yarnConf);
    Utils.initHdfsConf(hdfsConf);

    try {
      resourceFs = FileSystem.get(hdfsConf);
    } catch (IOException e) {
      LOG.error("Failed to create FileSystem object", e);
      return false;
    }

    hbMonitor.init(tonyConf);

    Options opts = Utils.getCommonOptions();
    CommandLine cliParser;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (ParseException e) {
      LOG.error("Got exception while parsing options", e);
      return false;
    }
    Map<String, String> envs = System.getenv();
    String[] shellEnvs = tonyConf.getStrings(TonyConfigurationKeys.EXECUTION_ENV);
    shellEnv = Utils.parseKeyValue(shellEnvs);
    String[] containerEnvs = tonyConf.getStrings(TonyConfigurationKeys.CONTAINER_LAUNCH_ENV);
    containerEnv.putAll(Utils.parseKeyValue(containerEnvs));

    appTimeout = tonyConf.getInt(TonyConfigurationKeys.APPLICATION_TIMEOUT,
                                 TonyConfigurationKeys.DEFAULT_APPLICATION_TIMEOUT);
    workerTimeout = tonyConf.getInt(TonyConfigurationKeys.WORKER_TIMEOUT,
        TonyConfigurationKeys.DEFAULT_WORKER_TIMEOUT);
    hdfsClasspath = cliParser.getOptionValue("hdfs_classpath");
    amRetryCount = tonyConf.getInt(TonyConfigurationKeys.AM_RETRY_COUNT,
        TonyConfigurationKeys.DEFAULT_AM_RETRY_COUNT);
    singleNode = Utils.getNumTotalTasks(tonyConf) == 0;
    secureMode = tonyConf.getBoolean(TonyConfigurationKeys.SECURITY_ENABLED,
        TonyConfigurationKeys.DEFAULT_SECURITY_ENABLED);
    enablePreprocessing = tonyConf.getBoolean(TonyConfigurationKeys.ENABLE_PREPROCESSING_JOB,
                                              TonyConfigurationKeys.DEFAULT_ENABLE_PREPROCESSING_JOB);
    containerId = ContainerId.fromString(envs.get(ApplicationConstants.Environment.CONTAINER_ID.name()));
    appIdString = containerId.getApplicationAttemptId().getApplicationId().toString();
    hbInterval = tonyConf.getInt(TonyConfigurationKeys.TASK_HEARTBEAT_INTERVAL_MS,
        TonyConfigurationKeys.DEFAULT_TASK_HEARTBEAT_INTERVAL_MS);
    maxConsecutiveHBMiss = tonyConf.getInt(TonyConfigurationKeys.TASK_MAX_MISSED_HEARTBEATS,
        TonyConfigurationKeys.DEFAULT_TASK_MAX_MISSED_HEARTBEATS);
    tonyHistoryFolder = tonyConf.get(TonyConfigurationKeys.TONY_HISTORY_LOCATION,
                                     TonyConfigurationKeys.DEFAULT_TONY_HISTORY_LOCATION);
    String distributedModeVal = tonyConf.get(TonyConfigurationKeys.APPLICATION_DISTRIBUTED_MODE,
            DEFAULT_APPLICATION_DISTRIBUTED_MODE);
    distributedMode = TonyConfigurationKeys.DistributedMode.valueOf(distributedModeVal.toUpperCase());

    try {
      historyFs = new Path(tonyHistoryFolder).getFileSystem(hdfsConf);
    } catch (IOException e) {
      LOG.error("Failed to create history FileSystem object", e);
      return false;
    }

    eventHandler = new EventHandler(historyFs, eventQueue);

    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.warn("Failed to fetch users", e);
    }
    return true;
  }

  private void buildTonySession() {
    TonySession.Builder builder = new TonySession.Builder()
        .setTonyConf(tonyConf)
        .setTaskExecutorJVMArgs(tonyConf.get(TonyConfigurationKeys.TASK_EXECUTOR_JVM_OPTS,
            TonyConfigurationKeys.DEFAULT_TASK_EXECUTOR_JVM_OPTS));
    sessionBuilder = builder;
    session = builder.build();
  }

  /**
   * Entry point of ApplicationMaster
   * The workflow of a training job in AM
   * prepare -> start -> failed    -> reset -> retry if amRetryCount > 0 otherwise fail the job.
   *                  -> succeeded -> stop -> job succeeded
   * @param args the args from user inputs
   */
  public static void main(String[] args) throws IOException {
    ApplicationMaster am = new ApplicationMaster();
    boolean succeeded = am.run(args);
    if (succeeded) {
      LOG.info("Application Master completed successfully. Exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. Exiting");
      System.exit(-1);
    }
  }

  private boolean run(String[] args) throws IOException {
    long started = System.currentTimeMillis();
    if (!init(args)) {
      return false;
    }

    if (!prepare()) {
      return false;
    }

    mainThread = Thread.currentThread();
    // Set up the builder with parameters that don't change
    JobMetadata.Builder metadataBuilder = new JobMetadata.Builder()
        .setId(appIdString)
        .setConf(yarnConf)
        .setStarted(started)
        .setUser(user);
    JobMetadata metadata = metadataBuilder.build();
    eventHandler.setUpThread(jobDir, metadata);
    eventHandler.start();
    boolean succeeded;
    do {
      // Crash AM on purpose during AM crash tests.
      String shouldCrash = System.getenv(Constants.TEST_AM_CRASH);
      if (shouldCrash != null && shouldCrash.equals("true")) {
        LOG.fatal("Error running ApplicationMaster !!");
        return false;
      }

      try {
        eventHandler.emitEvent(new Event(EventType.APPLICATION_INITED,
            new ApplicationInited(appIdString, Utils.getNumTotalTasks(tonyConf), Utils.getCurrentHostName()),
            System.currentTimeMillis()));
        start();
      } catch (Exception e) {
        LOG.error("Exception when we're starting TonyAM", e);
        return false;
      }

      // inject task plugin executor to run
      // TODO: 1/21/21 when retry, it should not start
      startPluginTasks();

      metricsReporter = new MetricsReporter(metricsRpcServer, appIdString, tonyConf);
      scheduledThreadPool.scheduleAtFixedRate(metricsReporter, 0, 1000 * 60, TimeUnit.MILLISECONDS);

      succeeded = monitor();
      if (succeeded || amRetryCount == 0) {
        LOG.info("Result: " + succeeded + ", retry count: " + amRetryCount);
        break;
      }

      // Prepare for retryCount.
      reset();
      LOG.info("Retrying, remaining retry count" + amRetryCount);
      amRetryCount -= 1;
    } while (!singleNode); // We don't retry on single node training.
    // Wait for the worker nodes to finish (The interval between registering the exit code to final exit)
    stop();
    long completed = System.currentTimeMillis();
    printTaskUrls();
    eventHandler.emitEvent(new Event(EventType.APPLICATION_FINISHED,
        new ApplicationFinished(appIdString, session.getNumCompletedTasks(),
            session.getNumFailedTasks(), new ArrayList<>()),
        System.currentTimeMillis()));
    metadata = metadataBuilder
        .setCompleted(completed)
        .setStatus(succeeded ? Constants.SUCCEEDED : Constants.FAILED)
        .build();
    eventHandler.stop(jobDir, metadata);

    return succeeded;
  }

  /**
   * Trigger plugin tasks. This part isn't shared across different retries.
   */
  private void startPluginTasks() {
    boolean enablePluginTasks = tonyConf.getBoolean(AM_PLUGIN_TASKS_ENABLED, DEFAULT_AM_PLUGIN_TASKS_ENABLED);
    LOG.info("Application master enable plugin tasks: " + enablePluginTasks);
    if (!enablePluginTasks) {
      return;
    }

    String[] pluginTasks = tonyConf.getStrings(AM_PLUGIN_TASKS, "TFMetricCollector");
    LOG.info("Application master plugin tasks: [" + StringUtils.join(pluginTasks, ",") + "]");
    if (null == pluginTasks) {
      return;
    }

    for (String pluginTaskName : pluginTasks) {
      switch (pluginTaskName) {
        case "TFMetricCollector":
          pluginExecutorsThreadPool.submit(new TFEvalMetricCollector(appIdString, session));
          pluginExecutorsThreadPool.submit(new TFTrainMetricCollector(appIdString, session));
          break;
        default:
          break;
      }
    }
  }

  /**
   * Prepare the application master. This part is shared across different retries.
   */
  private boolean prepare() throws IOException {
    LOG.info("Preparing application master..");

    NMCallbackHandler containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(yarnConf);
    nmClientAsync.start();

    // Setup application RPC server
    String amHostname = Utils.getCurrentHostName();
    applicationRpcServer = setupRPCService(amHostname);
    containerEnv.put(Constants.AM_HOST, amHostname);
    containerEnv.put(Constants.AM_PORT, Integer.toString(amPort));
    setOpalEnv(containerEnv);

    // Setup metrics RPC server.
    ServerSocket rpcSocket = new ServerSocket(0);
    int metricsRpcPort = rpcSocket.getLocalPort();
    rpcSocket.close();
    metricsRpcServer = new MetricsRpcServer();
    RPC.Builder metricsServerBuilder = new RPC.Builder(yarnConf).setProtocol(MetricsRpc.class)
        .setInstance(metricsRpcServer).setPort(metricsRpcPort);
    containerEnv.put(Constants.METRICS_RPC_PORT, Integer.toString(metricsRpcPort));

    // Init AMRMClient
    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(yarnConf);
    amRMClient.start();

    RegisterApplicationMasterResponse response;
    String hostNameOrIpFromTokenConf;
    String amHostPort;
    try {
      hostNameOrIpFromTokenConf = Utils.getHostNameOrIpFromTokenConf(yarnConf);
      String amTrackingUrl = startTensorFlowRedirectServer(amHostname, amPort);
      response = amRMClient.registerApplicationMaster(amHostname, amPort, amTrackingUrl);
      amHostPort = hostNameOrIpFromTokenConf + ":" + amPort;
    } catch (YarnException | SocketException e) {
      LOG.error("Exception while preparing AM", e);
      return false;
    }

    if (secureMode) {
      // Set up secret manager for RPC servers
      ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
      ClientToAMTokenIdentifier identifier = new ClientToAMTokenIdentifier(appAttemptID, user);
      byte[] secret = response.getClientToAMTokenMasterKey().array();
      ClientToAMTokenSecretManager secretManager = new ClientToAMTokenSecretManager(appAttemptID, secret);
      applicationRpcServer.setSecretManager(secretManager);
      metricsServerBuilder.setSecretManager(secretManager);

      // create token for application RPC server
      Token<? extends TokenIdentifier> tensorflowClusterToken = new Token<>(identifier, secretManager);
      tensorflowClusterToken.setService(new Text(amHostPort));
      UserGroupInformation.getCurrentUser().addToken(tensorflowClusterToken);

      // create token for metrics RPC server
      Token<? extends TokenIdentifier> metricsToken = new Token<>(identifier, secretManager);
      metricsToken.setService(new Text(hostNameOrIpFromTokenConf + ":" + metricsRpcPort));
      UserGroupInformation.getCurrentUser().addToken(metricsToken);

      setupContainerCredentials();
    }

    try {
      setupJobDir(historyFs, tonyHistoryFolder, appIdString);
      writeConfigFile(historyFs, jobDir);
    } catch (IOException e) {
      LOG.error(e);
      return false;
    }

    LOG.info("Starting application RPC server at: " + amHostPort);
    applicationRpcServer.start();

    LOG.info("Starting metrics RPC server at: " + amHostname + ":" + metricsRpcPort);
    RPC.Server metricsServer = metricsServerBuilder.build();
    if (yarnConf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      metricsServer.refreshServiceAclWithLoadedConfiguration(yarnConf, new TonyPolicyProvider());
    }
    metricsServer.start();

    hbMonitor.start();

    return true;
  }

  private String startTensorFlowRedirectServer(String hostname, int port) {
    String url = null;
    redirectServer = new TensorFlowRedirectServer();
    try {
      redirectServer.init(hostname, port);
      String amLogUrl = Utils.constructContainerUrl(hostname + ":"
          + System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name()), containerId);
      redirectServer.setAmLogUrl(amLogUrl);
      registerAmLogUrlOpal(amLogUrl);
      redirectServer.run();
      url = hostname + ":" + redirectServer.getPort();
    } catch (Exception e) {
      LOG.warn("Failed to start TensorFlow redirect server", e);
    }
    return url;
  }

  /**
   * Create job directory under intermediate folder.
   * @param fs FileSystem object.
   * @param histFolder History folder location string.
   * @param appId Application ID string.
   */
  private void setupJobDir(FileSystem fs, String histFolder, String appId) {
    Path interm = new Path(histFolder, Constants.TONY_HISTORY_INTERMEDIATE);
    try {
      if (!fs.exists(interm)) {
        LOG.error("Intermediate directory doesn't exist [" + interm.toString() + "]");
        return;
      }
    } catch (IOException e) {
      LOG.error("Failed to check intermediate directory existence", e);
      return;
    }

    jobDir = new Path(interm, appId);
    // set to `tony` group by default
    // due to inherited permission from parent folder
    Utils.createDirIfNotExists(fs, jobDir, Constants.PERM770);
  }

  /**
   * Generate config file in {@code jobDir} folder.
   * @param fs FileSystem object.
   * @param jobDir Path object of job directory (store all the files related to the job).
   * @throws IOException when failed to write config.xml to {@code jobDir}
   */
  private void writeConfigFile(FileSystem fs, Path jobDir) throws IOException {
    if (jobDir == null) {
      return;
    }
    Path configFile = new Path(jobDir, Constants.TONY_FINAL_XML);
    try (FSDataOutputStream out = fs.create(configFile)) {
      tonyConf.writeXml(out);
    } catch (IOException e) {
      throw new IOException("Failed to write config to XML", e);
    }
  }

  /**
   * This method start the training job. It also does the training preprocessing in this function as well
   * preprocessing job is used to abstract out common computation in each worker to a single place, however,
   * we do plan to move the preprocessing job to a worker node in the future to reduce the complexity of AM.
   * @throws IOException exception during HDFS file operations.
   * @throws InterruptedException during Thread.sleep.
   */
  private void start() throws Exception {

    int exitCode = 0;

    // Perform the preprocess job.
    if (enablePreprocessing || singleNode) {
       exitCode = doPreprocessingJob();
    }

    // Early exit for single node training.
    if (singleNode) {
      if (exitCode != 0) {
        LOG.info("Single node job exits with " + exitCode + ", exiting.");
        session.setFinalStatus(FinalApplicationStatus.FAILED, "Single node training failed..");
      } else {
        LOG.info("Single node job exits with " + exitCode + ", exiting.");
        session.setFinalStatus(FinalApplicationStatus.SUCCEEDED, "Single node job succeeded.");
      }
      return;
    }

    if (exitCode != 0) {
      return;
    }

    buildTonySession();
    scheduleTasks();
  }

  private void scheduleTasks() {
    session.setResources(yarnConf, hdfsConf, localResources, containerEnv, hdfsClasspath);
    List<TensorFlowContainerRequest> requests = session.getContainersRequests();
    for (TensorFlowContainerRequest request : requests) {
      scheduleTask(request);
    }
  }

  private void scheduleTask(TensorFlowContainerRequest request) {
    AMRMClient.ContainerRequest containerAsk = setupContainerRequestForRM(request);
    if (!jobTypeToContainerRequestsMap.containsKey(request.getJobName())) {
      jobTypeToContainerRequestsMap.put(request.getJobName(), new ArrayList<>());
    }
    jobTypeToContainerRequestsMap.get(request.getJobName()).add(containerAsk);
    amRMClient.addContainerRequest(containerAsk);
  }

  // Reset state to prepare for retryCount.
  private void reset() {
    List<Container> containers = sessionContainersMap.get(session.sessionId);
    for (Container container : containers) {
      nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
      LOG.info("Stop a task in container: containerId = " + container.getId() + ", containerNode = "
               + container.getNodeId().getHost());
    }

    // Reset session
    session = sessionBuilder.build();
    applicationRpcServer.reset();
    session.sessionId += 1;
  }

  /**
   * Monitor the TensorFlow training job.
   * @return if the tensorflow job finishes successfully.
   */
  private boolean monitor() {
    int attempt = 0;
    containerEnv.put(Constants.ATTEMPT_NUMBER, String.valueOf(attempt));
    long expireTime = appTimeout == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + appTimeout;
    int counter = 0;
    boolean evaluatorAlone = false;
    long evaluatorAloneStartTime = 0;
    long evaluatorAloneTimeout = tonyConf.getInt(TonyConfigurationKeys.EVALUATOR_RUN_ALONE_TIMEOUT,
        TonyConfigurationKeys.DEFAULT_EVALUATOR_RUN_ALONE_TIMEOUT);
    while (true) {
      counter += 1;
      // Checking timeout
      if (System.currentTimeMillis() > expireTime) {
        LOG.error("Application times out.");
        break;
      }

      // Check if client signals we should exit.
      if (clientSignalToStop) {
        LOG.info("Client signals AM to exit.");
        break;
      }

      if (session.isTrainingFinished()) {
        LOG.info("Training has finished.");
        break;
      }

      if (preprocessExitCode != 0) {
        LOG.error("Preprocess failed with exit code: " + preprocessExitCode);
        break;
      }

      if (singleNode && preprocessFinished) {
        LOG.info("Single node training finished with exit code: " + preprocessExitCode);
        break;
      }

      if (this.taskHasMissesHB) {
        LOG.error("Application failed due to missed heartbeats");
        break;
      }

      // Handle executor registered time out
      if (registrationTimeout()) {
        LOG.error("Application failed due to registered executor task timeout");
        break;
      }

      // Handle executor exit when launching task executor process
      if (startupFailed()) {
        LOG.error("Application failed due to started executor failed.");
        break;
      }

      int numTotalTrackedTasks = session.getTotalTrackedTasks();
      if (numTotalTrackedTasks > 0) {
        int numCompletedTrackedTasks = session.getNumCompletedTrackedTasks();
        if (numCompletedTrackedTasks == numTotalTrackedTasks) {
          Utils.printCompletedTrackedTasks(numCompletedTrackedTasks, numTotalTrackedTasks);
          break;
        } else if (evaluatorAloneTimeout > 0) {
          // Check if the evaluator is the only task
          int notCompletedEvaluatorTasks = session.getNotCompletedTrackedTasks("evaluator");
          if (!evaluatorAlone && notCompletedEvaluatorTasks > 0
              && notCompletedEvaluatorTasks == session.getNotCompletedTrackedTasks()) {
            LOG.info(String.format("All other job tasks finished, and only evaluators are left alone, " +
               "number of left evaluators: %d", notCompletedEvaluatorTasks));
            evaluatorAlone = true;
            evaluatorAloneStartTime = System.currentTimeMillis();
          }
          if (evaluatorAlone && System.currentTimeMillis() - evaluatorAloneStartTime >= evaluatorAloneTimeout) {
            String msg = String.format("All other job tasks finished, " +
                    "and evaluators have been running timeout for %d ms",
                System.currentTimeMillis() - evaluatorAloneStartTime);
            LOG.info(msg);
            session.setFinalStatus(FinalApplicationStatus.FAILED, msg);
            break;
          }
        }

        // Reduce logging frequency to every 100s.
        if (counter % 20 == 1) {
          Utils.printCompletedTrackedTasks(numCompletedTrackedTasks, numTotalTrackedTasks);
        }
      }

      // Pause before refresh job status
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.error("Thread interrupted", e);
      }
    }

    session.updateSessionStatus();
    FinalApplicationStatus status = session.getFinalStatus();
    String appMessage = session.getFinalMessage();
    if (status != FinalApplicationStatus.SUCCEEDED) {
      LOG.info("Tony session failed: " + appMessage);
    }
    return status == FinalApplicationStatus.SUCCEEDED;
  }

  private boolean startupFailed() {
    Set<TonyTask> completedFailedTasks = getCompletedFailedTasks();
    LOG.debug("Completed failed task size: " + completedFailedTasks.size());

    // When executor failed and not registering to AM, it means task failed when starting task executor process
    return completedFailedTasks.stream().anyMatch(t -> {
      String taskId = t.getTaskInfo().getName() + ":" + t.getTaskInfo().getIndex();
      LOG.debug("Failed task ID: " + taskId);
      boolean existFailed = !session.getRegisteredTasks().contains(t);
      if (existFailed) {
        String errorMsg = String.format("Stopping AM for task [%s:%s] starting failed, "
                        + "allocated container is %s on host %s",
                t.getJobName(), t.getTaskIndex(),
                (t.getContainer() != null ? t.getContainer().getId().toString() : "none"),
                (t.getContainer() != null ? t.getContainer().getNodeId().getHost() : "none"));
        LOG.error(errorMsg);
        session.setFinalStatus(FinalApplicationStatus.FAILED, errorMsg);
        stop();
      }
      return existFailed;
    });
  }

  private boolean registrationTimeout() {
    Set<TonyTask> unregisteredTasks = getUnregisteredTasks();
    for (TonyTask t : unregisteredTasks) {
      if (System.currentTimeMillis() - t.getStartTime() <= registrationTimeoutMs) {
        continue;
      }

      // If registration time out, need to stop all containers. And set the app failed
      String errorMsg = String.format("Stopping AM for task [%s:%s] registration timeout: "
                      + "allocated container is %s on host %s",
              t.getJobName(), t.getTaskIndex(),
              (t.getContainer() != null ? t.getContainer().getId().toString() : "none"),
              (t.getContainer() != null ? t.getContainer().getNodeId().getHost() : "none"));
      LOG.error(errorMsg);
      session.setFinalStatus(FinalApplicationStatus.FAILED, errorMsg);
      stop();
      return true;
    }

    return false;
  }

  /**
   * Returns the tasks whose containers have launched but not called {@link ApplicationRpc#registerWorkerSpec} yet.
   */
  private Set<TonyTask> getUnregisteredTasks() {
    return session.getTonyTasks().values().stream().flatMap(Arrays::stream)
        .filter(task -> task != null && task.getHost() == null)
        .collect(Collectors.toSet());
  }

  private Set<TonyTask> getCompletedFailedTasks() {
    return session.getTonyTasks().values().stream().flatMap(Arrays::stream)
        .filter(task -> task != null && task.isFailed())
        .collect(Collectors.toSet());
  }

  private void stop() {
    stopRunningContainers();

    FinalApplicationStatus status = session.getFinalStatus();
    String appMessage = session.getFinalMessage();
    try {
      amRMClient.unregisterApplicationMaster(status, appMessage, null);
    } catch (YarnException | IOException e) {
      LOG.error("Failed to unregister application", e);
    }

    redirectServer.stop();
    nmClientAsync.stop();
    amRMClient.stop();
    // Poll until TonyClient signals we should exit
    boolean result = Utils.poll(() -> clientSignalToStop, 1, 15);
    if (!result) {
      LOG.warn("TonyClient didn't signal Tony AM to stop.");
    }
  }

  /**
   * Stops any remaining running containers and gives them time to finish so we can collect their task metrics and emit
   * a TASK_FINISHED event.
   */
  private void stopRunningContainers() {
    List<Container> allContainers = sessionContainersMap.get(session.sessionId);
    if (allContainers != null) {
      for (Container container : allContainers) {
        TonyTask task = session.getTask(container.getId());
        if (!task.isCompleted()) {
          nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
        }
      }
    }

    // Give 15 seconds for containers to exit
    boolean result = Utils.poll(() -> session.getNumCompletedTasks() == session.getTotalTasks(), 1, 15);
    if (!result) {
      LOG.warn("Not all containers were stopped or completed. Only " + session.getNumCompletedTasks() + " out of "
          + session.getTotalTasks() + " finished.");
    }
  }

  // Run the preprocessing job and set up the common env variables for worker jobs.
  private int doPreprocessingJob() throws Exception {

    Utils.extractResources();
    HashMap<String, String> extraEnv = new HashMap<>(shellEnv);
    if (singleNode) {
      ServerSocket tbSocket = new ServerSocket(0);
      int tbPort = tbSocket.getLocalPort();
      extraEnv.put(Constants.TB_PORT, String.valueOf(tbPort));
      String tbUrl = Utils.getCurrentHostName() + ":" + tbPort;
      proxyUrl = Utils.constructUrl(tbUrl);
      LOG.info("Registering TensorBoard url for single node training: " + tbUrl);
      registerTensorBoardUrlToRM(tbUrl);
      tbSocket.close();
    }
    LOG.info("Start python preprocessing");

    extraEnv.put(Constants.PREPROCESSING_JOB, "true");

    /*
     * YARN sets $HOME to /user/yarn which users don't have access to write there.
     * Unfortunately, some services like Jupyter Notebook wants to write stuff there,
     * set it to user.dir (root of this container's address).
     */
    extraEnv.put("HOME", System.getProperty("user.dir"));
    setOpalEnv(extraEnv);
    String taskCommand = tonyConf.get(TonyConfigurationKeys.getExecuteCommandKey(Constants.AM_NAME),
                tonyConf.get(TonyConfigurationKeys.getContainerExecuteCommandKey()));
    LOG.info("Executing command: " + taskCommand);
    int exitCode = Utils.executeShell(taskCommand, workerTimeout, extraEnv);

    preprocessExitCode = exitCode;
    preprocessFinished = true;

    // Short circuit if preprocessing job fails.
    if (exitCode != 0) {
      LOG.error("Preprocess job exits with " + exitCode + ", exiting.");
      session.setFinalStatus(FinalApplicationStatus.FAILED, "Preprocessing job failed.");
      return exitCode;
    }
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(
        System.getProperty(YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR)
        + File.separatorChar + Constants.AM_STDOUT_FILENAME), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.contains("Model parameters: ")) {
          String params = line.substring(line.indexOf("Model parameters: ") + "Model parameters: ".length());
          // Add serialized params to env
          containerEnv.put(Constants.TASK_PARAM_KEY, params);
          break;
        }
      }
    }
    return exitCode;
  }

  private void printTaskUrls() {
    if (session != null) {
      session.getTonyTasks()
          .values()
          .stream()
          .flatMap(Arrays::stream)
          .forEach(task -> Utils.printTaskUrl(task.getTaskInfo(), LOG));
    }
  }

  private ApplicationRpcServer setupRPCService(String hostname) {
    ApplicationRpcServer rpcServer = new ApplicationRpcServer(hostname, new RpcForClient(), yarnConf);
    amPort = rpcServer.getRpcPort();
    return rpcServer;
  }

  private final class RpcForClient implements ApplicationRpc {
    private static final long REGISTRATION_STATUS_INTERVAL_MS = 15 * 1000;

    private long lastRegisterWorkerTime = System.currentTimeMillis();

    @Override
    public void reset() {
      session.resetRegisteredTasks();
    }

    @Override
    public Set<TaskInfo> getTaskInfos() {
      // Special handling for NotebookSubmitter.
      if (singleNode && proxyUrl != null) {
        HashSet<TaskInfo> additionalTasks = new HashSet<>();
        additionalTasks.add(new TaskInfo(Constants.DRIVER_JOB_NAME, "0", Utils.constructContainerUrl(
                          Utils.getCurrentHostName() + ":"
                          + System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name()), containerId)));
        additionalTasks.add(new TaskInfo(Constants.NOTEBOOK_JOB_NAME, "0", proxyUrl));
        return additionalTasks;
      }

      if (!singleNode && session != null && session.allTasksScheduled()) {
        return session.getTonyTasks().values().stream()
            .flatMap(tasks -> Arrays.stream(tasks).map(TonyTask::getTaskInfo))
            .collect(Collectors.toSet());
      }

      return Collections.emptySet();
    }

    @Override
    public String getClusterSpec() throws IOException {
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.writeValueAsString(session.getClusterSpec());
    }

    @Override
    public void taskExecutorHeartbeat(String taskId) {
      TonyTask task = session.getTask(taskId);
      if (task != null) {
        LOG.debug("[" + taskId + "] Received HB Ping !!");
        hbMonitor.receivedPing(task);
      } else {
        LOG.warn("[" + taskId + "] Not registered for heartbeat monitoring !!");
      }
    }

    @Override
    public String registerWorkerSpec(String taskId, String spec) throws IOException {
      TonyTask task = session.getTask(taskId);
      if (task.getHost() == null) {
        LOG.info("Received cluster spec registration request from task " + taskId + " with spec: " + spec);
        task.setHostPort(spec);
        session.addRegisteredTask(taskId);

        // HB Registration should happen only after worker registration..
        // The Task registration timeout will take care of rescheduling the task
        // on another node..
        LOG.info("[" + taskId + "] Received Registration for HB !!");
        hbMonitor.register(task);
        killChiefWorkerIfTesting(taskId);
      }

      metricsReporter.addTask(task);

      // two distributed mode(default is cluster) cases:
      // 1. when in independent-task mode, task will be allowed to run when AM accept worker register spec,
      // 2. when in cluster mode, it will start until all tasks have registered.
      switch (distributedMode) {
        case GANG:
          int totalTasks = session.getTotalTasks();
          if (session.getNumRegisteredTasks() == totalTasks) {
            LOG.info("All " + totalTasks + " tasks registered.");
            return getClusterSpec();
          } else {
            printTasksPeriodically();
            return null;
          }
        // return the current task spec directly.
        case FCFS:
          return spec;
        default:
          throw new IOException("Errors on registering to TonY AM, because of unknown distributed mode: "
                  + distributedMode);
      }
    }

    private void printTasksPeriodically() {
      // Periodically print a list of all tasks we are still awaiting registration from.
      if (System.currentTimeMillis() - lastRegisterWorkerTime > REGISTRATION_STATUS_INTERVAL_MS) {
        Set<TonyTask> unregisteredTasks = getUnregisteredTasks();

        int registeredTasksNum = session.getNumRegisteredTasks();
        LOG.info(String.format("Received registrations from %d tasks, awaiting registration from %d tasks.",
                registeredTasksNum, session.getTotalTasks() - registeredTasksNum));

        unregisteredTasks.forEach(t -> LOG.info(
                String.format("Awaiting registration from task %s %s in %s on host %s",
                        t.getJobName(), t.getTaskIndex(),
                        (t.getContainer() != null ? t.getContainer().getId().toString() : "none"),
                        (t.getContainer() != null ? t.getContainer().getNodeId().getHost() : "none")))
        );

        lastRegisterWorkerTime = System.currentTimeMillis();
      }
    }

    /**
     * This method was used to workaround an issue that the Python script finished while the container failed to
     * close due to GPU allocation issue, which doesn't exist anymore.
     *
     * Discussion: A benefit of decoupling registering execution result from container exit status is that we can decouple
     * tony from a specific resource manager's callback logic.
     * However, this easily introduces lots of bugs since we'd have 3 places to handle failures - register call, container
     * complete callback and heartbeat.
     * To make things easier, we decide to go back and piggyback on container completion to dictate the execution result
     * of a task. However, we use this method to unregister a completed task from the heartbeat monitor to avoid a race
     * condition where the container complete callback is delayed, too many heartbeats are missed, and the task is
     * marked as failed.
     */
    @Override
    public String registerExecutionResult(int exitCode, String jobName, String jobIndex, String sessionId) {
      LOG.info("Received result registration request with exit code " + exitCode + " from " + jobName + " " + jobIndex);

      // Unregister task after completion..
      // Since in the case of asynchronous exec, containers might
      // end at different times..
      TonyTask task = session.getTask(jobName + ":" + jobIndex);
      if (task != null) {
        LOG.info("Unregistering task [" + task.getId() + "] from Heartbeat monitor..");
        hbMonitor.unregister(task);
      } else {
        LOG.warn("Task " + jobName + " " + jobIndex + " was null!");
      }
      return "RECEIVED";
    }

    @Override
    public String registerTensorBoardUrl(String spec) throws Exception {
      LOG.info("Got request to update TensorBoard URL: " + spec);
      return registerTensorBoardUrlToRM(spec);
    }

    @Override
    public void finishApplication() {
      LOG.info("Client signals AM to finish application.");
      clientSignalToStop = true;
    }
  }

  private String registerTensorBoardUrlToRM(String spec) throws Exception {
    if (spec != null && appIdString != null) {
      try {
        redirectServer.setTensorBoardUrl("http://" + spec);
        LOG.info("TensorBoard URL: " + spec);
      } catch (Exception e) {
        LOG.warn("Failed to set TensorBoard URL to redirect server", e);
        return "FAILED";
      }
      return "SUCCEEDED";
    } else {
      return "FAILED";
    }
  }

  // Set up credentials for the containers.
  private void setupContainerCredentials() throws IOException {
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    String submitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
    UserGroupInformation submitterUgi = UserGroupInformation.createRemoteUser(submitterUserName);
    submitterUgi.addCredentials(credentials);
  }

  private AMRMClient.ContainerRequest setupContainerRequestForRM(TensorFlowContainerRequest request) {
    Priority priority = Priority.newInstance(request.getPriority());
    Resource capability = Resource.newInstance((int) request.getMemory(), request.getVCores());
    Utils.setCapabilityGPU(capability, request.getGPU());
    AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(capability, null, null, priority);
    LOG.info("Requested container ask: " + containerRequest.toString());
    return containerRequest;
  }

  private NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler();
  }

  /**
   * Node manager call back handler
   */
  class NMCallbackHandler implements NMClientAsync.CallbackHandler {
    @Override
    public void onContainerStopped(ContainerId containerId) {
      processFinishedContainer(containerId, ContainerExitStatus.KILLED_BY_APPMASTER);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
      LOG.info("Container Status: id =" + containerId + ", status =" + containerStatus);
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
      LOG.info("Successfully started container " + containerId);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start container " + containerId);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop container " + containerId);
    }
  }

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Completed containers: " + completedContainers.size());
      sleepForTesting();

      for (ContainerStatus containerStatus : completedContainers) {
        int exitStatus = containerStatus.getExitStatus();
        LOG.info("ContainerID = " + containerStatus.getContainerId()
            + ", state = " + containerStatus.getState()
            + ", exitStatus = " + exitStatus);
        String diagnostics = containerStatus.getDiagnostics();
        if (ContainerExitStatus.SUCCESS != exitStatus) {
          LOG.error(diagnostics);
        } else {
          LOG.info(diagnostics);
        }

        processFinishedContainer(containerStatus.getContainerId(), exitStatus);
      }
    }

    /**
     * For testing purposes to simulate delay of container completion callback.
     */
    private void sleepForTesting() {
      if (System.getenv(Constants.TEST_TASK_COMPLETION_NOTIFICATION_DELAYED) != null) {
        LOG.info("Sleeping for 1 second to simulate task completion notification delay");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.error(e);
        }
      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      LOG.info("Allocated: " + containers.size() + " containers.");
      for (Container container : containers) {
        LOG.info("Launching a task in container"
            + ", containerId = " + container.getId()
            + ", containerNode = " + container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
            + ", resourceRequest = " + container.getResource()
            + ", priority = " + container.getPriority());
        new ContainerLauncher(container).run();
      }
    }

    @Override
    public void onShutdownRequest() {
      LOG.info("onShutdownRequest called in RMCallbackHandler");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {
      LOG.info("onNodesUpdated called in RMCAllbackHandler");
    }

    @Override
    public float getProgress() {
      int numTotalTrackedTasks = session.getTotalTrackedTasks();
      return numTotalTrackedTasks > 0 ? (float) session.getNumCompletedTrackedTasks() / numTotalTrackedTasks : 0;
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error("Received error in AM to RM call", throwable);
      stop();
    }
  }

  /**
   * The command to prepare inside containers.
   */
  private class ContainerLauncher implements Runnable {
    Container container;

    ContainerLauncher(Container container) {
      this.container = container;
    }

    /**
     * Set up container's launch command and start the container.
     */
    public void run() {
      TonyTask task = session.getAndInitMatchingTaskByPriority(container.getPriority().getPriority());
      Preconditions.checkNotNull(task, "Task was null! Nothing to schedule.");

      task.setTaskInfo(container);
      TaskInfo taskInfo = task.getTaskInfo();
      taskInfo.setStatus(TaskStatus.READY);

      // Add job type specific resources
      Map<String, LocalResource> containerResources = new ConcurrentHashMap<>(localResources);
      String[] resources = tonyConf.getStrings(TonyConfigurationKeys.getResourcesKey(task.getJobName()));
      Utils.addResources(resources, containerResources, resourceFs);

      // All resources available to all containers
      resources = tonyConf.getStrings(TonyConfigurationKeys.getContainerResourcesKey());
      Utils.addResources(resources, containerResources, resourceFs);

      task.addContainer(container);
      LOG.info("Setting Container [" + container.getId() + "] for task [" + task.getId() + "]..");

      Map<String, String> containerLaunchEnv = new ConcurrentHashMap<>(containerEnv);

      /*
       * Add additional environment vars. We always set job_name task_index & task_num and
       * task_num and TaskExecutor is responsible for setting up the actual shell environment
       * for different deep learning frameworks.
       */
      String jobName = task.getJobName();
      String taskIndex = task.getTaskIndex();
      Map<String, String> dockerEnv = Utils.getContainerEnvForDocker(tonyConf, jobName);
      containerLaunchEnv.putAll(dockerEnv);
      containerLaunchEnv.put(Constants.JOB_NAME, jobName);
      containerLaunchEnv.put(Constants.TASK_INDEX, taskIndex);
      containerLaunchEnv.put(Constants.TASK_NUM, String.valueOf(session.getTotalTrackedTasks()));
      containerLaunchEnv.put(Constants.DISTRUBUTED_MODE_NAME, distributedMode.name());

      if (session.isChief(jobName, taskIndex)) {
        containerLaunchEnv.put(Constants.IS_CHIEF, Boolean.TRUE.toString());
      }
      // Specify session id in the env to distinguish between different sessions.
      containerLaunchEnv.put(Constants.SESSION_ID, String.valueOf(session.sessionId));

      List<CharSequence> arguments = new ArrayList<>(5);
      arguments.add(session.getTaskCommand());
      arguments.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      arguments.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
      StringBuilder command = new StringBuilder();
      for (CharSequence str : arguments) {
        command.append(str).append(" ");
      }
      List<String> commands = ImmutableList.of(command.toString());

      LOG.info("Constructed command: " + commands);
      LOG.info("Container environment: " + containerLaunchEnv);

      // Set logs to be readable by everyone.
      Map<ApplicationAccessType, String> acls = new HashMap<>(2);
      acls.put(ApplicationAccessType.VIEW_APP, "*");
      acls.put(ApplicationAccessType.MODIFY_APP, " ");

      ByteBuffer tokens = null;
      if (secureMode) {
        tokens = allTokens.duplicate();
      }
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(containerResources, containerLaunchEnv,
                                                                      commands, null, tokens, acls);

      sessionContainersMap.computeIfAbsent(session.sessionId, key ->
          Collections.synchronizedList(new ArrayList<>())
      ).add(container);

      Utils.printTaskUrl(task.getTaskInfo(), LOG);
      nmClientAsync.startContainerAsync(container, ctx);
      taskInfo.setStatus(TaskStatus.RUNNING);
      eventHandler.emitEvent(new Event(EventType.TASK_STARTED,
          new TaskStarted(task.getJobName(), Integer.parseInt(task.getTaskIndex()),
              container.getNodeHttpAddress().split(":")[0]),
          System.currentTimeMillis()));
    }
  }

  private void onTaskDeemedDead(TonyTask task) {
    String msg = "Task with id [" + task.getId() + "] has missed"
        + " [" + maxConsecutiveHBMiss + "] heartbeats. Ending application!";
    LOG.error(msg);
    taskHasMissesHB = true;
    session.setFinalStatus(FinalApplicationStatus.FAILED, msg);
    mainThread.interrupt();
  }

  private void processFinishedContainer(ContainerId containerId, int exitStatus) {
    TonyTask task = session.getTask(containerId);
    if (task != null) {
      // Ignore tasks from past sessions.
      if (task.getSessionId() != session.sessionId) {
        return;
      }

      LOG.info("Container " + containerId + " for task " + task + " finished with exitStatus " + exitStatus + ".");
      metricsReporter.deleteTask(task);
      session.onTaskCompleted(task.getJobName(), task.getTaskIndex(), exitStatus);
      eventHandler.emitEvent(new Event(EventType.TASK_FINISHED,
          new TaskFinished(task.getJobName(), Integer.parseInt(task.getTaskIndex()),
              task.getTaskInfo().getStatus().toString(),
              metricsRpcServer.getMetrics(task.getJobName(), Integer.parseInt(task.getTaskIndex()))),
          System.currentTimeMillis()));
    } else {
      LOG.warn("No task found for container : [" + containerId + "]!");
    }


  }

  private void setOpalEnv(Map<String, String> env) {
    env.put(Constants.APPLICATION_ID_STRING, appIdString);
    env.put(Constants.TONY_SUBMIT_TYPE, tonyConf.get(Constants.TONY_SUBMIT_TYPE_TAG, "cluster"));
    String opalUrl = Constants.OPAL_URL_PROD;
    if (tonyConf.getBoolean(Constants.TONY_OPAL_TEST, false)) {
      opalUrl = Constants.OPAL_URL_TEST;
    }
    env.put(Constants.OPAL_URL, opalUrl);

    String gearWorkflowId = tonyConf.get(Constants.GEAR_WORKFLOW_ID_KEY);
    if (StringUtils.isNotEmpty(gearWorkflowId)) {
      env.put(Constants.GEAR_WORKFLOW_ID_KEY, gearWorkflowId);
    }
  }

  //region testing
  private void killChiefWorkerIfTesting(String taskId) {
    // Simulation of chief worker been killed.
    if (System.getenv(Constants.TEST_WORKER_TERMINATED) != null && taskId.equals(Constants.COORDINATOR_ID)) {
      List<Container> containers = sessionContainersMap.get(session.sessionId);
      for (Container container : containers) {
        if (session.getTask(container.getId()).getJobName().equals(Constants.WORKER_JOB_NAME)) {
          LOG.warn("Simulating worker termination for taskId: " + taskId);
          nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
        }
      }
    }
  }
  //endregion

  private void registerAmLogUrlOpal(String amLogUrl) {
    String opalUrl = Constants.OPAL_URL_PROD;
    if (tonyConf.getBoolean(Constants.TONY_OPAL_TEST, false)) {
      opalUrl = Constants.OPAL_URL_TEST;
    }
    Set<TaskInfo> taskInfos = new HashSet<>();
    taskInfos.add(new TaskInfo("am", "0", amLogUrl));
    Gson gson = new Gson();
    String params = gson.toJson(taskInfos);
    try {
      String url = opalUrl + "/api/v1/tfjob/update/log?&appId=" + appIdString;
      url += "&token=" + Constants.OPAL_TOKEN;
      OpalUtils.httpPost(url, params);
    } catch (Exception e) {
      LOG.error("Failed to update task log info to opal", e);
    }
  }
}
