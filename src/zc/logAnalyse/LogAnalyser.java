package zc.logAnalyse;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;

import static zc.logAnalyse.ZCPatterns.*;
import static zc.logAnalyse.UnitConverter.*;

/**
 * 日志分析类
 * 首先分析RM的日志，然后分析NM到日志，接着分析AM的和Task的日志
 * 分析到方法是通过从日志中找到所需信息，然后对这些信息进行综合处理
 * 找所需信息到方法是用正则表达式模式匹配，正则表达式在ZCPatterns中统一定义
 * @author zc
 *
 */
public class LogAnalyser {
	
	public static Configuration conf; 

	private String LOG_HOME = null; //下载的日志文件夹，如 hadoopLogs_zc/
	private String RM_HOSTNAME = null; //RM 所在节点的主机名
	private String USERNAME = null; //hadoop 用户名
	private String RM_LOG_NAME = null; //RM 日志的文件名
	private String LOG_OUTPUT_DIR = null; //日志分析结果输出目录名

	private Map<String, AppAttempt> aaidToAppAttempt = null;
	private Map<String, TaskAttempt> taidToTaskAttempt = null;
	private Map<String, TaskContainer> cidToTaskContainer = null;
	private Map<String, String>	TCID2TaskLogName = null;
	
	private Set<String> AMLogNames = null;
	private Set<String> NMLogNames = null;
	private Set<String> nodeIds = null;
	
	private ZCMetrics metrics;
	
	public boolean outputUsageCurve = false;
	
	public static String hadoopConfDir;
	
	public static enum LogType {
		HADOOP, PREDRA, MROCHESTRATOR, ADMP
	}
	
	private LogType logType;

	public LogAnalyser() {
		this(null, null, null, null, false);
	}

	public LogAnalyser(String logHome, String rmHostName, String userName, LogType lt, boolean outputUsageCurve) {
		if (logHome != null)
			this.LOG_HOME = logHome;
		if (rmHostName != null)
			this.RM_HOSTNAME = rmHostName;
		if (userName != null)
			this.USERNAME = userName;
		this.RM_LOG_NAME = LOG_HOME + "/" + RM_HOSTNAME + "/yarn/yarn-"
				+ USERNAME + "-resourcemanager-" + RM_HOSTNAME + ".log";
		this.LOG_OUTPUT_DIR = LOG_HOME + "/analyse_result";

		//System.out.println("line 155: " + RM_LOG_NAME);
		
		this.aaidToAppAttempt = new HashMap<String, AppAttempt>();
		this.taidToTaskAttempt = new HashMap<String, TaskAttempt>();
		this.cidToTaskContainer = new HashMap<String, TaskContainer>();
		this.TCID2TaskLogName = new HashMap<String, String>();
		
		this.AMLogNames = new HashSet<String>();
		this.NMLogNames = new HashSet<String>();		
		this.nodeIds = new TreeSet<String>();
		this.logType = lt;
		this.outputUsageCurve = outputUsageCurve;
		
		String hadoopConfDir = logHome + "/etc/hadoop";
		
		if(conf == null) {
			conf = new Configuration();
			try {
				conf.addResource(new FileInputStream(hadoopConfDir + "/yarn-site.xml"));
				conf.addResource(new FileInputStream(hadoopConfDir + "/mapred-site.xml"));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		metrics = new ZCMetrics(aaidToAppAttempt,taidToTaskAttempt,cidToTaskContainer,conf);
	}

	public void analyse() {
		analyseRMLog(); // 分析RM日志
		analyseAMLogs(); // ...
		analyseNMLogs(); // ...
		//analyseTaskLogs();
		if(logType == LogType.PREDRA) analyseZCLogs();
	}

	// RM log name like
	// LOG_HOME/RM_HOSTNAME/yarn/yarn-zc-resourcemanager-slave1.log
	/**
	 * Includes two phases. First, extract information in log and build corresponding data structure.
	 * Second, analyze built data structures and output corresponding analyze result.
	 */
	public void analyseRMLog() {
		StringBuffer logBuffer = loadLogToBuffer(RM_LOG_NAME);
		if (logBuffer == null) {
			System.err
					.println("Can not load ResourceManager's log file. Please check the RM's file name or whether this log exists!");
			return;
		}
		// 首先通过AMLaunchPattern找到日志中所有的AM启动的日志记录
		// 对于每一条这样到记录，抽取出其中的信息，建立数据结构AppAttempt
		Matcher m = AMLaunchPattern.matcher(logBuffer);
		while (m.find()) {
			String launchTime = m.group(1);
			String appContainerId = m.group(2);
			String nodeId = m.group(3);
			String appContainerResource = m.group(4);
			String priority = m.group(5);
			String appAttemptId = m.group(6);
			String appId = m.group(7);
			AppAttempt a = new AppAttempt(appAttemptId, appId, appContainerId,
					appContainerResource, nodeId, priority, launchTime);
			this.addAppattempt(a);
			this.AMLogNames.add(this.getAMLogName(nodeId, a.getAppId(),
					appContainerId));
			this.nodeIds.add(nodeId);
		}
		Matcher m1 = appaSubmittedPattern.matcher(logBuffer);
		while(m1.find()) {
			AppAttempt a = getAppAttemptById(m1.group(2));
			if(a != null) {
				a.setSubmittedTime(m1.group(1));
			}
		}
		Matcher m2 = appaStartRunPattern.matcher(logBuffer);
		while(m2.find()) {
			AppAttempt a = getAppAttemptById(m2.group(2));
			if(a != null) {
				a.setStartRunTime(m2.group(1));
			}
		}
		Matcher m3 = appaFinishPattern.matcher(logBuffer);
		while(m3.find()) {
			AppAttempt a = getAppAttemptById(m3.group(2));
			if(a != null) {
				a.setFinishTime(m3.group(1));
			}
		}
		//  calculate sum wait time
		long sumWaitTime = 0l, sumTurnaroundTime = 0l;
		int jobCount = 0;
		for(AppAttempt a : aaidToAppAttempt.values()) {
			if(a.getFinishTime() < 0) {
				System.out.println("[* "+a.getAppAttemptId() + "][submit time: " + DateAndTime.timeToDate(a.getSubmittedTime()) 
						+ "][start time: " + DateAndTime.timeToDate(a.getStartRunTime()) 
						+ "][finish time: " + DateAndTime.timeToDate(a.getFinishTime()) + "]");
			}
			if(a.getStartRunTime() > metrics.getWaitAndTrunaroundStartTime() 
					&& a.getFinishTime() < metrics.getWaitAndTrunaroundEndTime() && a.getFinishTime() > 0) {
				jobCount ++;
				sumWaitTime += (a.getStartRunTime() - a.getSubmittedTime());
				sumTurnaroundTime += (a.getFinishTime() - a.getSubmittedTime());
				System.out.println("["+ a.getAppAttemptId() + "][submit time: " + DateAndTime.timeToDate(a.getSubmittedTime()) 
						+ "][start time: " + DateAndTime.timeToDate(a.getStartRunTime()) 
						+ "][finish time: " + DateAndTime.timeToDate(a.getFinishTime()) + "]");
			}
		}
		System.out.println("[total finished jobs: " + jobCount + "]");
		metrics.setTotalJobs(jobCount);
		metrics.setSumWaitTime(sumWaitTime);
		metrics.setSumTurnaroundTime(sumTurnaroundTime); 
	}

	public void analyseAMLogs() {
		
		for (String fileName : this.AMLogNames) {
			StringBuffer logBuffer = loadLogToBuffer(fileName);
			if (logBuffer == null)
				continue;
			Matcher m = aaidPattern.matcher(logBuffer);
			String appattemptId = null;
			if (m.find()) {
				appattemptId = m.group(1);
			} else {
				System.err.println("Can not find appattemptName in AM's Log!");
				return;
			}
			Matcher m1 = taAndCPattern.matcher(logBuffer);
			while (m1.find()) {
				String timestamp = m1.group(1);
				String taskAttemptId = m1.group(2);
				// String appid = m1.group(3);
				String taskType = m1.group(4);
				String taskContainerId = m1.group(5);
				String nodeId = m1.group(6);

				TaskAttempt ta = new TaskAttempt(taskAttemptId,
						taskContainerId, nodeId, appattemptId, taskType,
						timestamp);
				this.addTaskAttempt(ta);

				this.NMLogNames.add(this.getNMLogName(nodeId));
				String appId = getAppAttemptById(appattemptId).getAppId();
				this.TCID2TaskLogName.put(taskContainerId, getTaskLogName(nodeId, appId, taskContainerId));
			}
			Matcher m2 = taskAttemptStartRunPattern.matcher(logBuffer);
			while (m2.find()) {
				TaskAttempt ta = getTaskAttemptById(m2.group(2));
				if(ta != null) {
					ta.setStartTime(m2.group(1));
				}
			}
			Matcher m3 = taskAttemptSuccessPattern.matcher(logBuffer);
			while (m3.find()) {
				TaskAttempt ta = getTaskAttemptById(m3.group(2));
				if(ta != null) {
					ta.setFinishTime(m3.group(1));
				}
			}
		}
		
		
		// for analyze throughput
		// int taskCount = 0;
		// for calculate parallelism

	  long firstTaskStartTime = Long.MAX_VALUE;
	  long lastTaskFinishTime = 0l;
	  
	  // find the first task start time and the last task finish time.
	  for(TaskAttempt ta : taidToTaskAttempt.values()) {
	  	if(ta.timeValid()) {
	  		if(ta.getStartTime() < firstTaskStartTime) firstTaskStartTime = ta.getStartTime();
	  		if(ta.getFinishTime() > lastTaskFinishTime) lastTaskFinishTime = ta.getFinishTime();
	  	}
	  }
	  
	  System.out.println("[first task start time: " + DateAndTime.timeToDate(firstTaskStartTime) + "][last task finish time: " + DateAndTime.timeToDate(lastTaskFinishTime) + "]");
	  metrics.setFirstTaskStartTime(firstTaskStartTime);
	  metrics.setLastTaskFinishTime(lastTaskFinishTime); 
	  
	  // calculate max_parallelism: How many tasks are running at a certain time point.
		int max_parallelism = 0;
		int avg_parallelism = 0;
		long stepNum = 0;
		long sumPara = 0;
	  for(long i=firstTaskStartTime; i<=lastTaskFinishTime; i+=metrics.getParaStep()) {
	  	int paraCountInStep = 0;
	  	for(TaskAttempt ta : taidToTaskAttempt.values()) {
	  		if(ta.timeValid() && ta.getStartTime() <= i && ta.getFinishTime() >= i) {
	  			paraCountInStep ++;
	  		}
	  	}
	  	stepNum ++;
	  	sumPara += paraCountInStep;
	  	if(paraCountInStep > max_parallelism) max_parallelism = paraCountInStep;
	  	//System.out.println("current para: " + paraCountInStep);
	  }

	  metrics.setMaxParallelism(max_parallelism);
	  metrics.setAvgParallelism(((double)sumPara) / stepNum);
		//System.out.println("***** [Throughput] rate is " + metrics.getThroughputRateForWorkload() +", [parallelism] is " + max_parallelism);	
	}

	public void analyseNMLogs() {
		
		for (String fileName : this.NMLogNames) {
			StringBuffer logBuffer = loadLogToBuffer(fileName);
			if (logBuffer == null)
				continue;
			Matcher memUsageMatcher = null;
			switch(logType) {
			case HADOOP:
				memUsageMatcher = memUsePattern.matcher(logBuffer);
				break;
			case PREDRA:
				memUsageMatcher = memUsePattern2.matcher(logBuffer);
				break;
			case ADMP:
				memUsageMatcher = memUsePattern2.matcher(logBuffer);
				break;
			case MROCHESTRATOR:
				memUsageMatcher = memUsePattern2.matcher(logBuffer);
				break;
			default:	
			}
			while (memUsageMatcher.find()) {
				String timestamp = memUsageMatcher.group(1);
				String processTreeId = memUsageMatcher.group(2);
				String taskContainerId = memUsageMatcher.group(3);
				String memUsed = UnitConverter.toB(memUsageMatcher.group(4));
				String memTotal = UnitConverter.toB(memUsageMatcher.group(5));
				TaskContainer tc = this.getTaskContainerById(taskContainerId);
				if (tc != null) {
					tc.addMemUsageRecord(new MemUsage(timestamp, memUsed,
							memTotal));
					tc.setProcessTreeId(processTreeId);
				}
			}
		}
		for(TaskContainer tc : cidToTaskContainer.values()) {
			tc.getPoints();
		}
	  
	  // calculate avg memUseRate for all containers in system
		calculateAvgMemUseRate();
	  //System.out.println("***[avg mem use rate to allocation]: " + metrics.getMemUseRateToAlloc());
		calculateMemUserateSeries();
		calculateEstErrorCDF();
	
	}
	
  /**
   * calculate avg memUseRate for all containers in system
   */
	private void calculateAvgMemUseRate() {
	  int countOfValidRates = 0;
	  double maxMemUseRateForAllCoutainers = 0;
	  double sumMemUsed = 0;
	  double sumMemAlloc = 0;
	  double memUseRate = 0;
	  for(TaskContainer tc : cidToTaskContainer.values()) {
	  	maxMemUseRateForAllCoutainers = 
	  			tc.getMaxMemUseRate() > maxMemUseRateForAllCoutainers ? tc.getMaxMemUseRate() : maxMemUseRateForAllCoutainers;
	  	if(tc.getSumMemUsed() > 0 && tc.getSumMemAlloc() > 0) {
	  		sumMemUsed += tc.getSumMemUsed();
	  		sumMemAlloc += tc.getSumMemAlloc();
	  		countOfValidRates ++;
	  	}
	  }
	  if (countOfValidRates > 0 ) memUseRate = sumMemUsed / sumMemAlloc;
	  metrics.setMemUseRateToAlloc(memUseRate); // output memUseRate
	  
	}
	
	/**
	 * calculate estimation error CDF
	 */
	private void calculateEstErrorCDF() {
		if(logType == LogType.PREDRA) {
			for(TaskContainer tc : cidToTaskContainer.values()) {
				tc.analyseSimi();
				Map<Long, Long> memUsages = tc.getMemUsages();
				List<Map<Long, Long>> estimatedCurves = tc.getEstimatedCurves();
				if(estimatedCurves.size() > 0) {
					Map<Long, Long> lastCurve = estimatedCurves.get(estimatedCurves.size()-1);
					for(Long relativeTime : memUsages.keySet()) {
						long memUsage = memUsages.get(relativeTime);
						long memEst = lastCurve.get(tc.getAbsTime(relativeTime));
						metrics.addEUDiff(memEst-memUsage);
					}
				}
			}
		}
	}
	
	/**
	 * @deprecated 
	 * calculate estimation error rate
	 */
	@Deprecated
	private void calculateEstErrorRate() {
		int tcCount = 0;
		double avgDiff = 0;
		double avgAbsDiff = 0;
		for(TaskContainer tc : cidToTaskContainer.values()) {
			tc.analyseSimi();
			tcCount ++;
			avgDiff += tc.getAvgDiff();
			avgAbsDiff += tc.getAvgAbsDiff();
		}
		if(tcCount != 0) {
			avgDiff /= tcCount;
			avgAbsDiff /= tcCount;
			metrics.setAvgDiff(avgDiff);
			metrics.setAvgAbsDiff(avgAbsDiff);
			System.out.println("******avgDiff: " + avgDiff + ", avgAbsDiff: " + avgAbsDiff);
		}
	}
	
	//calculate alloc mem use rate: the sum of memory used of all the running tasks at a certain time point divide the alloc mem.
	private void calculateMemUserateSeries() {
		if(logType == LogType.HADOOP) {
		  for(long i=metrics.getFirstTaskStartTime(); i<=metrics.getLastTaskFinishTime(); i+=metrics.getMemUsageStep()) {
		  	long sumMemUsageAtTheTime = 0;
		  	long sumMemAllocAtTheTime = 0;
		  	double maxMemUsageRateAtTheTime = 0;
		  	for(TaskAttempt ta : taidToTaskAttempt.values()) {
		  		if(ta.timeValid() && ta.getStartTime() <= i && ta.getFinishTime() >= i) {
		  			long memUsageOfTheTask = ta.getTaskContainer().getMemUsage(i);
		  			long memAllocOfTheTask = ta.getTaskContainer().getMemAlloc(i);
		  			sumMemUsageAtTheTime += memUsageOfTheTask;
		  			sumMemAllocAtTheTime += memAllocOfTheTask;
		  			double currUseRate = (double)memUsageOfTheTask / memAllocOfTheTask;
		  			maxMemUsageRateAtTheTime = currUseRate > maxMemUsageRateAtTheTime ? currUseRate : maxMemUsageRateAtTheTime;
		  		}
		  	}
		  	metrics.addMemUAPair(i, (long)BtoMB(sumMemUsageAtTheTime), (long)BtoMB(sumMemAllocAtTheTime));
		  }
		}
	}
	
	@Deprecated
	public void analyseTaskLogs() {
		for(Map.Entry<String, String> entry : TCID2TaskLogName.entrySet()) {
			String taskContainerId = entry.getKey();
			String fileName = entry.getValue();
			StringBuffer logBuffer = loadLogToBuffer(fileName);
			if ( logBuffer == null ) continue;
			Matcher start = jvmMonitorStartPattern.matcher(logBuffer);
			if(start.find()) {
				JvmMemUsage jmu = new JvmMemUsage(start.group(1), start.group(2), start.group(3));
				getTaskContainerById(taskContainerId).addJvmUsageRecord(jmu);
			}
			Matcher m1 = jvmMemUsePattern.matcher(logBuffer);
			while(m1.find()) {
				JvmMemUsage jmu = new JvmMemUsage(m1.group(1), m1.group(5), m1.group(6));
				getTaskContainerById(taskContainerId).addJvmUsageRecord(jmu);
			}
			Matcher m2 = mapCompletionPattern.matcher(logBuffer);
			while(m2.find()) {
				GMark gm = new GMark(m2.group(1), GMark.mapComplete, Integer.parseInt(m2.group(5)));
				getTaskContainerById(taskContainerId).addGMarkRecord(gm);
			}
			Matcher m3 = copyPhaseEndPattern.matcher(logBuffer);
			if(m3.find()) {
				GMark gm = new GMark(m3.group(1), GMark.copyPhaseEnd, -1);
				getTaskContainerById(taskContainerId).addGMarkRecord(gm);
			}
			Matcher m4 = sortPhaseStartPattern.matcher(logBuffer);
			if(m4.find()) {
				GMark gm = new GMark(m4.group(1), GMark.sortPhaseStart, -1);
				getTaskContainerById(taskContainerId).addGMarkRecord(gm);
			}
			Matcher m5 = finalMergeStartPattern.matcher(logBuffer);
			if(m5.find()) {
				GMark gm = new GMark(m5.group(1), GMark.finalMergeStart, -1);
				getTaskContainerById(taskContainerId).addGMarkRecord(gm);
			}
			Matcher m6 = sortPhaseCompletePattern.matcher(logBuffer);
			if(m6.find()) {
				GMark gm = new GMark(m6.group(1), GMark.sortPhaseEnd, -1);
				getTaskContainerById(taskContainerId).addGMarkRecord(gm);
			}
		}
	}
	
	public void analyseZCLogs() {
		String zcResourceLimitEstimatorLogName = "zc-info-in-ResourceLimitEstimator.log";
		String zcNMSchedulerLogName = "zc-test-zc-NMScheduler.log";
		
		int regCounter = 0;
		int failedRegCounter = 0;
		
		int startCounter = 0;
		int preemptCounter = 0;
		
		for(String nodeId : nodeIds) {
			String zcLogDir = getZCLogDir(nodeId);
			String zcRLELogPath = zcLogDir + "/" + zcResourceLimitEstimatorLogName;
			String zcNMSLogPath = zcLogDir + "/" + zcNMSchedulerLogName;
			StringBuffer zcRLELog = loadLogToBuffer(zcRLELogPath);
			StringBuffer zcNMSLog = loadLogToBuffer(zcNMSLogPath);
			Matcher mFailedReg = zcFailedRegressionPattern.matcher(zcRLELog);
			while(mFailedReg.find()) {
				regCounter += Integer.parseInt(mFailedReg.group(1));
				failedRegCounter += 1;
			}
			Matcher mPreempt = zcPreemptionPattern.matcher(zcNMSLog);
			while(mPreempt.find()) {
				startCounter += Integer.parseInt(mPreempt.group(1));
				preemptCounter += Integer.parseInt(mPreempt.group(2));
			}
		}
		float preemptionRate = startCounter == 0 ? 0 : (float)preemptCounter / startCounter;
		float failedRegRate = regCounter == 0 ? 0 : (float)failedRegCounter / regCounter;
		metrics.setPreemptionRate(preemptionRate);
		metrics.setFailRegRate(failedRegRate);
		//System.out.println("[Preemption rate]: " + preemptionRate + ", [failed regression rate]: " + failedRegRate);
	}

	public static StringBuffer loadLogToBuffer(String fileName) {

		File f = new File(fileName);
		if (f.exists()) {
			BufferedReader br = null;
			StringBuffer logBuffer = new StringBuffer();
			try {
				br = new BufferedReader(new FileReader(f));
			} catch (FileNotFoundException e) {
				System.err.println("Can not find file. ");
				return null;
			}
			String oneline = null;
			try {
				while ((oneline = br.readLine()) != null) {
					logBuffer.append(oneline);
					logBuffer.append("\n");
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			return logBuffer;
		}
		System.err.println("Can not find file.");
		return null;
	}

	private String getAMLogName(String nodeId, String appId,
			String appContainerId) {
		return LOG_HOME + "/" + nodeId + "/app/" + appId + "/" + appContainerId
				+ "/syslog";
	}

	private String getNMLogName(String nodeId) {
		return LOG_HOME + "/" + nodeId + "/yarn/yarn-" + USERNAME
				+ "-nodemanager-" + nodeId + ".log";
	}
	
	private String getZCLogDir(String nodeId) {
		return LOG_HOME + "/" + nodeId + "/zcLogs";
	}
	
	private String getTaskLogName(String nodeId, String appId, String taskContainerId) {
		return LOG_HOME + "/"+ nodeId + "/app/"+ appId + "/" + taskContainerId + "/syslog";
	}

	private AppAttempt getAppAttemptById(String aaid) {
		return this.aaidToAppAttempt.get(aaid);
	}

	private TaskContainer getTaskContainerById(String tcid) {
		return this.cidToTaskContainer.get(tcid);
	}

	private TaskAttempt getTaskAttemptById(String taid) {
		return this.taidToTaskAttempt.get(taid);
	}

	private void addAppattempt(AppAttempt appattempt) {
		this.aaidToAppAttempt.put(appattempt.getAppAttemptId(), appattempt);
	}

	private void addTaskAttempt(TaskAttempt taskAttempt) {
		this.aaidToAppAttempt.get(taskAttempt.getAppattemptId())
				.addTaskAttempt(taskAttempt);
		this.taidToTaskAttempt.put(taskAttempt.getTaskAttemptId(), taskAttempt);
		this.cidToTaskContainer.put(taskAttempt.getTaskContainerId(),
				taskAttempt.getTaskContainer());
	}

	@Deprecated
	public void print() {
		for (Map.Entry<String, AppAttempt> entry : aaidToAppAttempt.entrySet()) {
			System.out.println(entry.getValue());
		}
		System.out.println(this.AMLogNames.toString());
		System.out.println(this.NMLogNames.toString());
	}

	@Deprecated
	public void print(BufferedWriter wr) {
		
		try {
			wr.write("\n\n------------------Scanning starts from ResoureManager's log-----------------\n\n");
			wr.write(this.RM_LOG_NAME);
			wr.write("\n\n------------------Then goes to the ApplicationMaster's log------------------\n\n");
			wr.write(this.AMLogNames.toString());
			wr.write("\n\n--------------------Finally gets to the NodeManager's log-------------------\n\n");
			wr.write(this.NMLogNames.toString());
			wr.write("\n\n----------------------------------------------------------------------------\n");
			for (Map.Entry<String, AppAttempt> entry : aaidToAppAttempt
					.entrySet()) {
				wr.write(entry.getValue().toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void outputResult() {
		File f = new File(LOG_OUTPUT_DIR);
		if (!f.exists())
			f.mkdir();
		for (AppAttempt appa : aaidToAppAttempt.values()) {
			appa.outputResult(LOG_OUTPUT_DIR);
		}
	}
	
	public void outputMetrics() {
		//String metricsOut = "LOG_OUTPUT_DIR/metrics";
		//metrics.serialize(metricsOut, logType);
		metrics.serialize(logType);
	}

	/**
	 * TODO input workload characters.
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		LogAnalyser la = null;
		if (args.length < 5) {
			//la = new LogAnalyser(
			//		"/home/zc/Desktop/smallTools/logAnalysisTools/hadoopLogs_zc",
			//		"centos-1", "zc", LogType.HADOOP, false);
			System.err.println("Arguments should be larger than 5. "
					+ "\n downloadedLogSourcePath rmlogDir username [hadoop|predra|mror|admp] [true(outputUsageCurve)|false]");
			return;
		} else {
			String type = args[3];
			LogType typee = null;
			switch(type) {
			case "hadoop":
				typee = LogType.HADOOP;
				break;
			case "predra":
				typee = LogType.PREDRA;
				break;
			case "mror":
				typee = LogType.MROCHESTRATOR;
				break;
			case "admp":
				typee = LogType.ADMP;
				break;
			}
			la = new LogAnalyser(args[0],args[1],args[2],typee,Boolean.parseBoolean(args[4]));
		}
		la.analyse();
		//BufferedWriter bw = new BufferedWriter(new FileWriter("/home/zc/Desktop/smallTools/logAnalysisTools/logLog.log"));
		//la.print(bw);
		//bw.close();
		if(la.outputUsageCurve) la.outputResult();
		la.outputMetrics();
	}
}
