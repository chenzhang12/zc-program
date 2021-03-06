package zc.logAnalyse;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import zc.logAnalyse.LogAnalyser.LogType;

public class ZCMetrics {
	
	private Map<String, AppAttempt> aaidToAppAttempt = null;
	private Map<String, TaskAttempt> taidToTaskAttempt = null;
	private Map<String, TaskContainer> cidToTaskContainer = null;
	private Configuration conf = null;
	
	// Estimation and Usage differences
	private int maxDiffMB = Integer.MIN_VALUE;
	private int minDiffMB = Integer.MAX_VALUE;
	private long sumDiffCount = 0l;
	private Map<Integer, Long> eudiffCounts;
	private Map<Integer, Float> CDF;
	
	// common
  private long firstTaskStartTime = Long.MAX_VALUE;
  private long lastTaskFinishTime = 0l;
	
	////// throughput //////
	private long throughputStartTime = 0;
	private long throughputEndTime = Long.MAX_VALUE - 1;
	private double throughputRate;
	private int totalJobs;
	
	////// wait and turnaround /////
	private long waitAndTrunaroundStartTime = 0;
	private long waitAndTrunaroundEndTime = Long.MAX_VALUE - 1;
	private long sumWaitTime;
	private long sumTurnaroundTime;

	////// parallelism //////
	private long paraStep = 1000; // 1000ms by default
	private int maxParallelism;
	private double avgParallelism;
	
	////// memory usage //////
	private long memUsageStep = 60000;
	private long memUsageStep2 = 3000;
	private double avgMemUseRateInCluster;
	private double maxMemUseRateInCluster;
	private double memUseRateToAlloc;
	private long totalMemInSys = 4l * 1024 * 1024 * 1024 * 5; // 4G / node, 5 compute node.
	// used for store memory usage and allocation time series
	private Map<Long, Pair<Long, Long>> memUASeries = null;
	private Map<String, Map<Long, Pair<Long, Long>>> node2MemUASeries = null;
	private Map<Long, UAScTuple> collectedUAScs = null;
	
	////// curve precision //////
	private double avgDiff;
	private double avgAbsDiff;
	
	///// preemption rate /////
	private double preemptionRate;
	private double failRegRate;
	
	public ZCMetrics(Map<String, AppAttempt> aaidToAppAttempt,
			             Map<String, TaskAttempt> taidToTaskAttempt,
			             Map<String, TaskContainer> cidToTaskContainer,
			             Configuration conf) {
		this.conf = conf;
		this.aaidToAppAttempt = aaidToAppAttempt;
		this.taidToTaskAttempt = taidToTaskAttempt;
		this.cidToTaskContainer = cidToTaskContainer;
		memUASeries = new TreeMap<>();
		node2MemUASeries = new HashMap<>();
		collectedUAScs = new TreeMap<>();
		eudiffCounts = new TreeMap<>();
		CDF = new TreeMap<>();
	}

	public long getThroughputStartTime() {
		return throughputStartTime;
	}

	public void setThroughputStartTime(long throughputStartTime) {
		this.throughputStartTime = throughputStartTime;
	}

	public long getThroughputEndTime() {
		return throughputEndTime;
	}

	public void setThroughputEndTime(long throughputEndTime) {
		this.throughputEndTime = throughputEndTime;
	}

	public int getTotalJobs() {
		return totalJobs;
	}

	public void setTotalJobs(int jobNum) {
		this.totalJobs = jobNum;
	}

	@Deprecated
	public double getThroughputRate() {
		return ((double)totalJobs) / (throughputEndTime - throughputStartTime);
	}
	
	public double getThroughputRateForWorkload() {
		return ((double)totalJobs) / getWorkloadDuration();
	}
	
	public double getThroughputRatePerHour() {
		return getThroughputRateForWorkload() * 1000 * 3600;
	}

	public long getWaitAndTrunaroundStartTime() {
		return waitAndTrunaroundStartTime;
	}

	public void setWaitAndTrunaroundStartTime(long waitAndTrunaroundStartTime) {
		this.waitAndTrunaroundStartTime = waitAndTrunaroundStartTime;
	}

	public long getWaitAndTrunaroundEndTime() {
		return waitAndTrunaroundEndTime;
	}

	public void setWaitAndTrunaroundEndTime(long waitAndTrunaroundEndTime) {
		this.waitAndTrunaroundEndTime = waitAndTrunaroundEndTime;
	}

	public long getParaStep() {
		return paraStep;
	}

	public void setParaStep(long paraStep) {
		this.paraStep = paraStep;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	public void setMaxParallelism(int maxParallelism) {
		this.maxParallelism = maxParallelism;
	}
	
	public void setAvgParallelism(double avgPara) {
		avgParallelism = avgPara;
	}
	
	public double getAvgParallelism() {
		return avgParallelism;
	}

	public long getSumWaitTime() {
		return sumWaitTime;
	}

	public void setSumWaitTime(long sumWaitTime) {
		this.sumWaitTime = sumWaitTime;
	}
	
	public double getAvgWaitTime() {
		return (double)sumWaitTime / totalJobs; 
	}

	public long getSumTurnaroundTime() {
		return sumTurnaroundTime;
	}

	public void setSumTurnaroundTime(long sumTurnaroundTime) {
		this.sumTurnaroundTime = sumTurnaroundTime;
	}
	
	public double getAvgTurnaroundTime() {
		return (double)sumTurnaroundTime / totalJobs;
	}

	public long getFirstTaskStartTime() {
		return firstTaskStartTime;
	}

	public void setFirstTaskStartTime(long firstTaskStartTime) {
		this.firstTaskStartTime = firstTaskStartTime;
	}

	public long getLastTaskFinishTime() {
		return lastTaskFinishTime;
	}

	public void setLastTaskFinishTime(long lastTaskFinishTime) {
		this.lastTaskFinishTime = lastTaskFinishTime;
	}

	public long getMemUsageStep() {
		return memUsageStep;
	}

	public long getMemUsageStep2() {
		return memUsageStep2;
	}

	public void setMemUsageStep(long memUsageStep) {
		this.memUsageStep = memUsageStep;
	}

	public double getAvgMemUseRateInCluster() {
		return avgMemUseRateInCluster;
	}

	public void setAvgMemUseRateInCluster(double avgMemUseRateInCluster) {
		this.avgMemUseRateInCluster = avgMemUseRateInCluster;
	}

	@Deprecated
	public double getMaxMemUseRateInCluster() {
		return maxMemUseRateInCluster;
	}

	@Deprecated
	public void setMaxMemUseRateInCluster(double maxMemUseRateInCluster) {
		this.maxMemUseRateInCluster = maxMemUseRateInCluster;
	}

	public long getTotalMemInSys() {
		return totalMemInSys;
	}

	public void setTotalMemInSys(long totalMemInSys) {
		this.totalMemInSys = totalMemInSys;
	}

	public double getMemUseRateToAlloc() {
		return memUseRateToAlloc;
	}

	public void setMemUseRateToAlloc(double memUseRateToAlloc) {
		this.memUseRateToAlloc = memUseRateToAlloc;
	}

	public double getAvgDiff() {
		return avgDiff;
	}

	public void setAvgDiff(double avgDiff) {
		this.avgDiff = avgDiff;
	}

	public double getAvgAbsDiff() {
		return avgAbsDiff;
	}

	public void setAvgAbsDiff(double avgAbsDiff) {
		this.avgAbsDiff = avgAbsDiff;
	}
	
	public long getWorkloadDuration() {
		return getLastTaskFinishTime() - getFirstTaskStartTime(); 
	}

	public double getPreemptionRate() {
		return preemptionRate;
	}

	public void setPreemptionRate(double preemptionRate) {
		this.preemptionRate = preemptionRate;
	}

	public double getFailRegRate() {
		return failRegRate;
	}

	public void setFailRegRate(double failRegRate) {
		this.failRegRate = failRegRate;
	}
	
	/**
	 * @param time abs time
	 * @param memUsed Unit MB
	 * @param memAlloc Unit MB
	 */
	public void addMemUAPair(long time, long memUsed, long memAlloc) {
		memUASeries.put(time, new Pair<Long, Long>(memUsed, memAlloc));
	}
	
	public void addUAScTuple(long sctime, long u, long a, int sc) {
		UAScTuple tuple = collectedUAScs.get(sctime);
		if(tuple == null) {
			tuple = new UAScTuple(u,a,sc);
			collectedUAScs.put(sctime, tuple);
		} else {
			tuple.add(u,a,sc);
		}
	}
	
	public void addNid2MemUAPair(String nodeId, long time, long memUsed, long memAlloc) {
		Map<Long, Pair<Long, Long>> series = node2MemUASeries.get(nodeId);
		if(series == null) {
			series = new TreeMap<Long, Pair<Long, Long>>();
			node2MemUASeries.put(nodeId, series);
		}
		series.put(time, new Pair<Long, Long>(memUsed, memAlloc));
	}
	
	public Map<Long, Pair<Long, Long>> getMemUASeries() {
		return memUASeries;
	}
	
	public Map<String, Map<Long, Pair<Long, Long>>> getNid2MemUASeries() {
		return node2MemUASeries;
	}
	
	public void addEUDiff(long diff) {
		int diffMB = (int)toMB(diff);
		sumDiffCount ++;
		if(diffMB > maxDiffMB) maxDiffMB = diffMB;
		if(diffMB < minDiffMB) minDiffMB = diffMB;
		Long count = eudiffCounts.get(diffMB);
		if(count == null || count.longValue() == 0) {
			eudiffCounts.put(diffMB, 1l);
		} else {
			eudiffCounts.put(diffMB, count.longValue() + 1);
		}
	}
	
	private void convertToCDF() {
		long cumulatedCount = 0;
		for(Map.Entry<Integer, Long> entry : eudiffCounts.entrySet()) {
			long countOfDiffMB = entry.getValue();
			cumulatedCount += countOfDiffMB;
			float ratio = (float)((double)cumulatedCount) / sumDiffCount;
			CDF.put(entry.getKey(), ratio);
		}
	}
	
	private long toMB(long B) {
		double MB = ((double)B) / 1024 / 1024;
		long rMB = Math.round(MB);
		return rMB;
	}
	
	@Deprecated
	public void serialize(String outDir, LogType logType) {
		String simpleMetricsFile = outDir + "/simpleMetrics";
		
		BufferedWriter bw1 = null;
		try {
			bw1 = new BufferedWriter(new FileWriter(simpleMetricsFile));
			// serialize normal metrics
			String outInfo = "Throughput=" + getThroughputRateForWorkload() + "\n"
					           + "AJWT=" + getAvgWaitTime() + "\n"
					           + "AJTT=" + getAvgTurnaroundTime() + "\n"
					           + "AP=" + getAvgParallelism() + "\n"
					           + "MUR=" + getMemUseRateToAlloc() + "\n"
					           + "PPREM=" + getPreemptionRate() + "\n"
					           + "PFR=" + getFailRegRate() + "\n";
			bw1.write(outInfo);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bw1.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if(logType == LogType.PREDRA) {
			// serialize CDF
			convertToCDF();
			// write out CDF
			String cdfMetricFile = outDir + "/CDF";
			try {
				bw1 = new BufferedWriter(new FileWriter(cdfMetricFile));
				bw1.write("DiffOfMEMU "); bw1.write("CDFOfDiff\n");
				for(Map.Entry<Integer, Float> entry : CDF.entrySet()) {
					bw1.write(entry.getKey() + " ");
					bw1.write(entry.getValue() + "\n");
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					bw1.close();
				} catch (IOException e) {e.printStackTrace();}
			}
	
		}

	}
	
	private String getMapReq() {
		return conf.get("mapreduce.map.memory.mb");
	}
	
	private String getReduceReq() {
		return conf.get("mapreduce.reduce.memory.mb");
	}
	
	private float getDelta() {
		return Float.parseFloat(conf.get("yarn.nodemanager.estimator.increase.threshold-ratio.to-MI"));
	}
	
	private float getBeta() {
		return Float.parseFloat(conf.get("yarn.nodemanager.estimator.increase.increment-ratio.to-UM"));
	}
	
	private float getGamma() {
		return Float.parseFloat(conf.get("yarn.nodemanager.estimator.release.threshold-ratio.to-UM"));
	}
	
	private float getSigma() {
		return Float.parseFloat(conf.get("yarn.nodemanager.estimator.release.slowdown"));
	}
	
	public void serialize(LogType logType) {
		
		String title = "\n[title]\n"
				+ logType.toString() + " " + getMapReq() + " " + getReduceReq() + " " + getDelta() + " " + getBeta() + " " + getGamma() + " " + getSigma();
		System.out.println(title);
		
		String outInfo = "\n[metrics]\n"
	      + "TP(perHour)=" + getThroughputRatePerHour() + "\n"
        + "AJWT=" + getAvgWaitTime() + "\n"
        + "AJTT=" + getAvgTurnaroundTime() + "\n"
        + "AP=" + getAvgParallelism() + "\n"
        + "MUR=" + getMemUseRateToAlloc() + "\n"
        + "PPREM=" + getPreemptionRate() + "\n"
        + "PFR=" + getFailRegRate();
		System.out.println(outInfo);
		
		if (memUASeries != null && memUASeries.size() != 0) {
			System.out.println("\n[UASeries]");			
			System.out.println("time(abs) memUsed memAlloc"); // separated by spaces
			for(Map.Entry<Long, Pair<Long, Long>> entry : memUASeries.entrySet()) {
				long time = entry.getKey();
				if(time > 0) {
					long memUsed = entry.getValue().getKey(); // unit MB
					long memAlloc = entry.getValue().getValue(); // unit MB
					if(memAlloc >= 0) {
						System.out.println(time + " " + memUsed + " " + memAlloc); // separated by spaces
					}
				}
			}
		}
		
		if (collectedUAScs != null & collectedUAScs.size() != 0) {
			System.out.println("\n[collectedUAScs]");
			System.out.println("time(abs) memUsed memAlloc sched"); // separated by spaces
			for(Map.Entry<Long, UAScTuple> entry : collectedUAScs.entrySet()) {
				long time = entry.getKey();
				if(time > 0) {
					UAScTuple tp = entry.getValue();
					long memUsed = tp.u; // unit MB
					long memAlloc = tp.a; // unit MB
					int sc = tp.sc;
					if(memAlloc >= 0) {
						System.out.println(time + " " + memUsed + " " + memAlloc + " " + sc); // separated by spaces
					}
				}
			}
		}
	
		if(logType == LogType.PREDRA) {
			if(eudiffCounts != null && eudiffCounts.size() != 0) {
				convertToCDF();
				System.out.println("\n[CDF]");
				System.out.println("DiffOfMEMU(MB) CDFOfDiff");
				for(Map.Entry<Integer, Float> entry : CDF.entrySet()) {
					System.out.println(entry.getKey() + " " + entry.getValue());
				}
			}
		}
	}
	
}
