package zc.logAnalyse;

import java.util.regex.Pattern;

public class ZCPatterns {
	
	/*
	 * ///////////////////////////////////// pattern strings //////////////////////////////////////
	 */

	private static final String infoLogPrefix = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO";
	
	
	private static final String taAndCPatternStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO \\[AsyncDispatcher event handler\\] org\\.apache\\.hadoop\\.mapreduce\\.v2\\.app\\.job\\.impl\\.TaskAttemptImpl: TaskAttempt: \\[(attempt_(\\d+_\\d+)_([m,r])_\\d+_\\d+)\\] using containerId: \\[(container_\\d+_\\d+_\\d+_\\d+) on NM: \\[([a-z,A-Z,0-9,-]+):\\d+\\]";
	private static final String cLaunchPatternStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO org\\.apache\\.hadoop\\.yarn\\.server\\.nodemanager\\.DefaultContainerExecutor: launchContainer: \\[nice, -n, \\d, bash, /home/\\w+/data/yarn/local/usercache/\\w+/appcache/application_\\d+_\\d{4}/(container_\\d+_\\d+_\\d+_\\d+)/default_container_executor\\.sh\\]";
	private static final String memUsePatternStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO org\\.apache\\.hadoop\\.yarn\\.server\\.nodemanager\\.containermanager\\.monitor\\.ContainersMonitorImpl: Memory usage of ProcessTree (\\d+) for container-id (container_\\d+_\\d+_\\d+_\\d+): ([\\d,\\.]+ [G,M]B) of ([\\d,\\.]+ [G,M]B) physical";
	private static final String memUsePatternStr2 = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO org\\.apache\\.hadoop\\.yarn\\.server\\.nodemanager\\.containermanager\\.monitor\\.ContainersMonitorImpl: Memory usage of ProcessTree (\\d+) for container-id (container_\\d+_\\d+_\\d+_\\d+): (\\d+)B of (\\d+)B physical";
	private static final String cStopPatternStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO org\\.apache\\.hadoop\\.yarn\\.server\\.nodemanager\\.containermanager\\.ContainerManagerImpl: Stopping container with container Id: (container_\\d+_\\d+_\\d+_\\d{6})";
	private static final String jvmMonitorStartPatternStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO \\[main\\] org\\.apache\\.hadoop\\.mapred\\.YarnChild: Child starting: ([\\d,\\.]+ [\\w]B) of ([\\d,\\.]+ [\\w]B) jvm memory used";
	private static final String jvmMemUsePatternStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO \\[jvm_mem_monitor\\] org\\.apache\\.hadoop\\.mapred\\.YarnChild: Jvm Memory usage of task (attempt_(\\d+_\\d+)_([m,r])_\\d+_\\d+): ([\\d,\\.]+ [\\w]B) of ([\\d,\\.]+ [\\w]B) jvm memory used";

	/*
	 * recognize gmark patterns in syslog
	 */
	private static final String mapCompletionPStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO \\[EventFetcher for fetching Map Completion Events\\] org\\.apache\\.hadoop\\.mapreduce\\.task\\.reduce\\.EventFetcher: (attempt_(\\d+_\\d+)_([m,r])_\\d+_\\d+): Got (\\d+) new map-outputs";	
	private static final String copyPhaseEndStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO \\[main\\] org\\.mortbay\\.log: copy phase complete";	
	private static final String sortPhaseStartStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO \\[main\\] org\\.mortbay\\.log: sort phase start";
	private static final String finalMergeStartStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO \\[main\\] org\\.apache\\.hadoop\\.mapreduce\\.task\\.reduce\\.MergeManagerImpl: start final merge";
	private static final String sortPhaseCompleteStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO \\[main\\] org\\.apache\\.hadoop\\.mapred\\.ReduceTask: sort phase complete";
	private static final String concurrencyStr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO org\\.apache\\.hadoop\\.yarn\\.server\\.nodemanager\\.containermanager\\.monitor\\.ContainersMonitorImpl: Number of running containers on this node is (\\d+)";
	
 
	
	////////////////////////// states of applications begin ///////////////////////////////
	
	// application attempt id pattern string
	private static final String aaidPatternStr = "Created MRAppMaster for application (appattempt_\\d+_\\d+_\\d{6})";

	// e.g. 2014-11-09 11:34:21,152 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl: application_1415503922159_0001 State change from ACCEPTED to RUNNING
	private static final String appStartRunStr = infoLogPrefix + " org\\.apache\\.hadoop\\.yarn\\.server\\.resourcemanager\\.rmapp\\.RMAppImpl: (application_\\d+_\\d+) State change from ACCEPTED to RUNNING";
	
	// 2014-11-09 11:34:12,713 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl: appattempt_1415503922159_0001_000001 State change from NEW to SUBMITTED
	private static final String appaSubmittedStr = infoLogPrefix + " org\\.apache\\.hadoop\\.yarn\\.server\\.resourcemanager\\.rmapp\\.attempt\\.RMAppAttemptImpl: (appattempt_\\d+_\\d+_\\d{6}) State change from NEW to SUBMITTED";
	
	// 2014-11-09 11:34:12,765 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl: appattempt_1415503922159_0001_000001 State change from SUBMITTED to SCHEDULED
	private static final String appaScheduledStr = infoLogPrefix + " org\\.apache\\.hadoop\\.yarn\\.server\\.resourcemanager\\.rmapp\\.attempt\\.RMAppAttemptImpl: (appattempt_\\d+_\\d+_\\d{6}) State change from SUBMITTED to SCHEDULED";
	// 2014-11-09 11:34:12,798 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl: appattempt_1415503922159_0001_000001 State change from SCHEDULED to ALLOCATED_SAVING
	
	// 2014-11-09 11:34:12,809 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl: appattempt_1415503922159_0001_000001 State change from ALLOCATED_SAVING to ALLOCATED
	
	//  2014-11-09 11:34:13,169 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl: appattempt_1415503922159_0001_000001 State change from ALLOCATED to LAUNCHED
  //AM launch pattern string
	private static final String AMLaunchPatternStr = infoLogPrefix + " org\\.apache\\.hadoop\\.yarn\\.server\\.resourcemanager\\.amlauncher\\.AMLauncher: Done launching container Container: \\[ContainerId: (container_\\d+_\\d+_\\d+_\\d+), NodeId: ([a-z,A-Z,0-9,-]+):\\d+, NodeHttpAddress: [a-z,A-Z,0-9,-]+:\\d+, Resource: (<memory:[\\d,\\.]+, vCores:\\d+>), Priority: (\\d+), Token: Token \\{ kind: ContainerToken, service: \\d+\\.\\d+\\.\\d+\\.\\d+:\\d+ \\}, \\] for AM (appattempt_(\\d+_\\d+)_\\d{6})";

	// 2014-11-09 11:34:21,151 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl: appattempt_1415503922159_0001_000001 State change from LAUNCHED to RUNNING
	private static final String appaStartRunStr = infoLogPrefix + " org\\.apache\\.hadoop\\.yarn\\.server\\.resourcemanager\\.rmapp\\.attempt\\.RMAppAttemptImpl: (appattempt_\\d+_\\d+_\\d{6}) State change from LAUNCHED to RUNNING";
	
	// 2014-11-09 11:34:33,593 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl: appattempt_1415503922159_0001_000001 State change from FINAL_SAVING to FINISHING
	// 2014-11-09 12:18:27,955 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl: appattempt_1415504548970_0004_000001 State change from FINISHING to FINISHED
	private static final String appaFinishStr = infoLogPrefix + " org\\.apache\\.hadoop\\.yarn\\.server\\.resourcemanager\\.rmapp\\.attempt\\.RMAppAttemptImpl: (appattempt_\\d+_\\d+_\\d{6}) State change from FINISHING to FINISHED";

	// 2014-11-09 11:34:33,593 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl: application_1415503922159_0001 State change from FINAL_SAVING to FINISHING
	private static final String appFinishStr = infoLogPrefix + " org\\.apache\\.hadoop\\.yarn\\.server\\.resourcemanager\\.rmapp\\.RMAppImpl: (application_\\d+_\\d+) State change from FINAL_SAVING to FINISHING";
	
	// [appattempt_1425705353847_0089_000001][submit time: 2015-03-07 05:37:55,664][start time: 2015-03-07 07:08:35,744][finish time: 2015-03-07 07:11:13,782]
	private static final String zcAResultAppAttemptStr = "\\[(appattempt_.+)\\]\\[submit time: (.+)\\]\\[start time: (.+)\\]\\[finish time: (.+)\\]";
	////////////////////////// states of tasks begin ///////////////////////////
	
	// 2014-11-09 12:20:38,985 INFO [AsyncDispatcher event handler] org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl: attempt_1415504548970_0007_m_000007_0 TaskAttempt Transitioned from ASSIGNED to RUNNING
	private static final String taskAttemptStartRunningStr = infoLogPrefix + " \\[AsyncDispatcher event handler\\] org\\.apache\\.hadoop\\.mapreduce\\.v2\\.app\\.job\\.impl\\.TaskAttemptImpl: (attempt_(\\d+_\\d+)_([m,r])_\\d+_\\d+) TaskAttempt Transitioned from ASSIGNED to RUNNING";

	// 2014-11-09 12:20:53,621 INFO [AsyncDispatcher event handler] org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl: attempt_1415504548970_0007_m_000010_0 TaskAttempt Transitioned from SUCCESS_CONTAINER_CLEANUP to SUCCEEDED
	private static final String taskAttemptSuccessStr = infoLogPrefix + " \\[AsyncDispatcher event handler\\] org\\.apache\\.hadoop\\.mapreduce\\.v2\\.app\\.job\\.impl\\.TaskAttemptImpl: (attempt_(\\d+_\\d+)_([m,r])_\\d+_\\d+) TaskAttempt Transitioned from SUCCESS_CONTAINER_CLEANUP to SUCCEEDED";
 
	private static final String zcPreemptionStr = "Preempt done, started (\\d+) now, preempted (\\d+) now\\.";
	
	private static final String zcFailedRegressionStr = "Estimate count now is (\\d+), failed estimate count now is 1\\.";

	
	/*
	 * ///////////////////////////////////////// patterns //////////////////////////////////////////
	 */
	
	
	/**
	 * group(1): 2014-11-09 11:34:21,152
	 * group(2): application_1415503922159_0001
	 */
	public static final Pattern appStartRunPattern = Pattern.compile(appStartRunStr);
	
	/**
	 * group(1): 2014-11-09 11:34:12,713
	 * group(2): appattempt_1415503922159_0001_000001
	 */
	public static final Pattern appaSubmittedPattern = Pattern.compile(appaSubmittedStr);
	
	/**
	 * group(1): 2014-11-09 11:34:12,765
	 * group(2): appattempt_1415503922159_0001_000001
	 */
	public static final Pattern appaScheduledPattern = Pattern.compile(appaScheduledStr);
	
	/**
	 * group(1): 2014-11-09 11:34:21,151
	 * group(2): appattempt_1415503922159_0001_000001
	 */
	public static final Pattern appaStartRunPattern = Pattern.compile(appaStartRunStr);
	
	/**
	 * group(1): 2014-11-09 11:34:33,593
	 * group(2): appattempt_1415503922159_0001_000001
	 */
	public static final Pattern appaFinishPattern = Pattern.compile(appaFinishStr);
	
	/**
	 * group(1): 2014-11-09 11:34:33,593
	 * group(2): application_1415503922159_0001
	 */
	public static final Pattern appFinishPattern = Pattern.compile(appFinishStr);
	
	/**
	 * AMLaunchPattern: Launching container for Applicationmaster. (in RM's log)
	 * group1: timeStamp group2: appContainerId group3: nodeId group4:
	 * appContainerResource group5: priority group6: appAttemptId group7: appId
	 * ( = "application_" + group7)
	 * 
	 * When this pattern is met, create object AppAttempt and add to List.
	 * Generate the AM log names and record them for later scanning.
	 */
  public static final Pattern AMLaunchPattern = Pattern.compile(AMLaunchPatternStr);
  
	/**
	 * aaidPattern: appattemptId (in AM's log ) group1: appattemptId
	 * 
	 * When this pattern is met, record appAttemptId which would be added in the
	 * TaskAttempt to be created.
	 */
  public static final Pattern aaidPattern = Pattern.compile(aaidPatternStr);
  
	/**
	 * taAndCPattern: relationship between task attempts and their containers
	 * (in AM's log) group1: timestamp group2: taskAttemptId group3: appid
	 * (deprecated) group4: taskType group5: taskContainerId group6: nodeId
	 * 
	 * When this pattern is met, create object TaskAttempt and object
	 * TaskContainer which will be attached to the former one. Generate the NM
	 * log names and record them for later scanning.
	 */
  public static final Pattern taAndCPattern = Pattern.compile(taAndCPatternStr);
  
	/**
	 * cLaunchPattern: task container launching pattern (in NM's log) group1:
	 * timestamp group2: taskContainerId
	 * 
	 * When this pattern is met, the initial value of memory usage of
	 * taskContainer created before will be set.
	 */
  public static final Pattern cLaunchPattern = Pattern.compile(cLaunchPatternStr);
  
	/**
	 * memUsePattern: memory usage information of each container (in NM's log)
	 * group1: timestamp group2: processTreeId group3: taskContainerId group4:
	 * memory used group5: memory total
	 * 
	 * When this pattern is met, the memory usage information of formerly
	 * created taskContainer will be added to it. The processTreeId will be set.
	 */
  public static final Pattern memUsePattern = Pattern.compile(memUsePatternStr);
  
  /**
   *  group1: timestamp 
   *  group2: processTreeId 
   *  group3: taskContainerId 
   *  group4: memory used (without unit. default B)
   *  group5: memory total (without unit. default B)
   */
  public static final Pattern memUsePattern2 = Pattern.compile(memUsePatternStr2);
  
	/**
	 * cStopPattern: task container stopping pattern (in NM's log) group1:
	 * timestamp group2: taskContainerId
	 * 
	 * whem this pattern is met, the memory usage information of taskContainer
	 * created before will be added with a final value.
	 */
  public static final Pattern cStopPattern = Pattern.compile(cStopPatternStr);
  
	/**
	 * log line that matches this pattern is written by zc.
	 * it is logged in the task attempt container's log.
	 * when this pattern is matched, remember to create the jvm mem triple, and add it to corresponding container.
	 */
  public static final Pattern jvmMemUsePattern = Pattern.compile(jvmMemUsePatternStr);
  
	/**
	 * log line that matches this pattern is written by zc.
	 * it is logged in the task attempt container's log.
	 * when this pattern is matched, the jvm mem logging info is going to occur.
	 * So remember to create the "zero" jvm mem triple, and add it to corresponding container.
	 */
  public static final Pattern jvmMonitorStartPattern = Pattern.compile(jvmMonitorStartPatternStr);
  public static final Pattern mapCompletionPattern = Pattern.compile(mapCompletionPStr);
  public static final Pattern copyPhaseEndPattern = Pattern.compile(copyPhaseEndStr);
  public static final Pattern sortPhaseStartPattern = Pattern.compile(sortPhaseStartStr);
  public static final Pattern finalMergeStartPattern = Pattern.compile(finalMergeStartStr);
  public static final Pattern sortPhaseCompletePattern = Pattern.compile(sortPhaseCompleteStr);
  public static final Pattern concurrencyPattern = Pattern.compile(concurrencyStr);
  
	/**
	 * group(1): 2014-11-09 12:21:16,206
	 * group(2): attempt_1415504548970_0007_m_000016_0
	 * group(3): 1415504548970_0007
	 * group(4): m
	 */
  public static final Pattern taskAttemptSuccessPattern = Pattern.compile(taskAttemptSuccessStr);
  /**
   * group(1): 2014-11-09 12:20:38,978
   * group(2): attempt_1415504548970_0007_m_000001_0
   * group(3): 1415504548970_0007
   * group(4): m
   */
  public static final Pattern taskAttemptStartRunPattern = Pattern.compile(taskAttemptStartRunningStr);
  
  public static final Pattern zcPreemptionPattern = Pattern.compile(zcPreemptionStr);

  public static final Pattern zcFailedRegressionPattern = Pattern.compile(zcFailedRegressionStr);
  
  /**
   * group(1) aaid
   * group(2) submit time
   * group(3) start time
   * group(4) finish time
   */
  public static final Pattern zcAResultAppAttemptPattern = Pattern.compile(zcAResultAppAttemptStr);
}
