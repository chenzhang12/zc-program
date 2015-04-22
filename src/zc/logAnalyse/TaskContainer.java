package zc.logAnalyse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.resourceestimator.Logarithm2;

public class TaskContainer {
	
	private String taskContainerId = null; // group(5);
	private String processTreeId = null;
	private List<MemUsage> memUsageRecords = null; // list of triple <timestamp,
																									// mem used, mem total>.
	private long sumMemUsed = 0;
	private long sumMemAlloc = 0;
	private double curMaxMemUseRateToAlloc = 0;
	private TreeMap<Long, Long> memUsages = null;
	private List<JvmMemUsage> jvmUsageRecords = null; // list of triple
																										// <timestamp, mem used, mem
																										// total>.
	private List<GMark> gmarks = null;
	
	private Logarithm2 logarithm;
	
	private long earliestTime = Long.MAX_VALUE;
	
	// ZC ///////////////////////////////////////

	
	private List<Map<Long,Long>> estimatedCurves;
	double avgDiff = 0;
	double avgAbsDiff = 0;
	/////////////////////////////////////////////////////////////////

	public TaskContainer(String taskContainerId, Configuration conf) {
		this.taskContainerId = taskContainerId;
		this.memUsageRecords = new ArrayList<MemUsage>();
		this.jvmUsageRecords = new ArrayList<JvmMemUsage>();
		//this.memUsages = new TreeMap<Long,Long>();
		this.estimatedCurves = new ArrayList<Map<Long,Long>>();
		this.gmarks = new ArrayList<GMark>();
		logarithm = new Logarithm2(conf);
	}

	public List<MemUsage> getUsageRecords() {
		return memUsageRecords;
	}
	
	public List<JvmMemUsage> getJvmUsageRecords() {
		return jvmUsageRecords;
	}
	
	public List<GMark> getGMarks() {
		return gmarks;
	}

	public String getTaskContainerId() {
		return taskContainerId;
	}

	public String getProcessTreeId() {
		return processTreeId;
	}

	public void setProcessTreeId(String processTreeId) {
		if (this.processTreeId == null)
			this.processTreeId = processTreeId;
	}

	public void addMemUsageRecord(MemUsage record) {
		this.memUsageRecords.add(record);
		this.sumMemUsed += record.getMemUsedWithoutUnit();
		this.sumMemAlloc += record.getMemAllocWithoutUnit();
		double memUsedRate = (double)record.getMemUsedWithoutUnit() / record.getMemAllocWithoutUnit();
		curMaxMemUseRateToAlloc = memUsedRate > curMaxMemUseRateToAlloc ? memUsedRate : curMaxMemUseRateToAlloc;
	}
	
	public List<MemUsage> getMemUsageRecords() {
		return memUsageRecords;
	}
	
	public void addJvmUsageRecord(JvmMemUsage record) {
		this.jvmUsageRecords.add(record);
	}
	
	public void addGMarkRecord(GMark record) {
		this.gmarks.add(record);
	}
	
	/**
	 * 1. Generate sampled estimated curves.
	 * 2. analyze the similarity of estimated and real curve.
	 */
	public void analyseSimi() {
		TreeMap<Long, Long> allPoints = memUsages;		
		// Sample at most five points. The interval of sample is at least 3.
		Long sampleTimePoints[] = sample(allPoints, 10, 3);
		// Calculate the estimate curve using the points between
  	// first point in the records and the sampled points.
		if(sampleTimePoints != null) {
		// Get estimated used memory at all record time point 
		// based on each of the estimate curves corresponding to each sample above.
			for(int i=0; i<sampleTimePoints.length - 1; ++i) {
				Map<Long, Long> sampledPoints = getPointsBySample(allPoints, sampleTimePoints[i], sampleTimePoints[i+1]);
				// update function
				boolean success = logarithm.updateFunction(sampledPoints);
				// get and record all the estimated memory usage point sets according to the updated function.
				if(success) {
					Map<Long,Long> estimatedCurve = new TreeMap<>();
					for(Long time : allPoints.keySet()) {
						long mem = logarithm.getValue(time);
						estimatedCurve.put(getAbsTime(time), mem);
					}
					estimatedCurves.add(estimatedCurve);
				}
			}
		}
		// ZCTODO: analyze the similarity of estimated and real curve.
		/*
		int curveNum = estimatedCurves.size();
		avgDiff = 0;
		avgAbsDiff = 0;
		for(Map<Long,Long> curve : estimatedCurves) {
			int pointNum = curve.size();
			long sumOfDiffInCurve = 0l;
			long absSumOfDiffInCurve = 0l;
			long sumOfRealInCurve = 0l;
			for(Map.Entry<Long, Long> point : curve.entrySet()) {
				Long realValue = allPoints.get(point.getKey() - earliestTime);
				if(realValue != null) {
					double diff = point.getValue().longValue() - realValue.longValue();
					sumOfDiffInCurve += diff;
					absSumOfDiffInCurve += Math.abs(diff);
					sumOfRealInCurve += realValue.longValue();
				}
			}
			if(pointNum != 0) {
				avgDiff += sumOfDiffInCurve / (double)sumOfRealInCurve;
				avgAbsDiff += absSumOfDiffInCurve / (double)sumOfRealInCurve;
			}		
		}
		if(curveNum != 0) {
			avgDiff /= curveNum;
			avgAbsDiff /= curveNum;
			//System.out.println("******avgDiff: "+avgDiff+ ", avgAbsDiff: " + avgAbsDiff);
		}		
		*/
	}
	
	/**
	 * Using relative time in records
	 */
	public Map<Long, Long> getMemUsages() {
		return memUsages;
	}
	
	/**
	 * Using absolute time in records
	 * absoluteTime == getAbsTime(relativeTime)
	 */
	public List<Map<Long, Long>> getEstimatedCurves() {
		return estimatedCurves;
	}
	
	@Deprecated
	public double getAvgAbsDiff() {
		return avgAbsDiff;
	}
	@Deprecated
	public double getAvgDiff() {
		return avgDiff;
	}
	
	@Deprecated
	public double getMemUseRate() {
		long sumMemUse = 0;
		long sumMemAlloc = 0;
		for(MemUsage record : memUsageRecords) {
			sumMemUse += record.getMemUsedWithoutUnit();
			sumMemAlloc += record.getMemAllocWithoutUnit();
		}
		if(sumMemUse != 0 && sumMemAlloc != 0)
			return sumMemUse / (double) sumMemAlloc;
		return -1;
	}
	
	public double getMaxMemUseRate() {
		return curMaxMemUseRateToAlloc;
	}
	
	public long getSumMemUsed() {
		return sumMemUsed;
	}
	
	public long getSumMemAlloc() {
		return sumMemAlloc;
	}
	
	public List<Map<Long,Long>> getSampledCurves() {
		return estimatedCurves;
	}
	
	/**
	 * get memUsage NEAR the time
	 * @param time abstime
	 * @return memused or -1 where there is no memusage recorded near given time
	 */
	public long getMemUsage(long time) {
		time -= earliestTime;
		Map.Entry<Long, Long> entry = memUsages.ceilingEntry(time);
		if(entry == null) entry = memUsages.floorEntry(time);
		if(entry != null) return entry.getValue();
		return -1;
	}
	
	// Get Points before the sampled time. Regard points as sorted by time.
	@Deprecated
	private Map<Long, Long> getPointsBySample(Map<Long, Long> points,
			Long sampleTime) {
		Map<Long, Long> sampledPoints = new TreeMap<>();
		for(Map.Entry<Long, Long> point : points.entrySet()) {
			if(point.getKey() > sampleTime) break;
			sampledPoints.put(point.getKey(), point.getValue());
		}
		return sampledPoints;
	}
	
	// Get point that between the two sampled times. (sampleT1 <= t < sampleT2)
	private Map<Long, Long> getPointsBySample(Map<Long, Long> points, Long sampledT1, Long sampledT2) {
		Map<Long, Long> sampledPoints = new TreeMap<>();
		for(Map.Entry<Long, Long> point : points.entrySet()) {
			if(point.getKey() < sampledT1) continue;
			if(point.getKey() >= sampledT2) break;
			sampledPoints.put(point.getKey(), point.getValue());
		}
		return sampledPoints;
	}

	/*
	 * Sample a sample set no greater than maxSampleNum with difference of number
	 * between each two adjacent sample elements no less than minSampleInterval.
	 * The first sample interval usually larger than the rest. 
	 */
	private Long[] sample(Map<Long, Long> points, int maxSampleNum, int minSampleInterval) {
		// get points number
		int pointsNum = points.size();
		Long timeArray[] = new Long[pointsNum];
		points.keySet().toArray(timeArray);
		// determine sample number
		int sampleNum = maxSampleNum;
		while(sampleNum > 0) {
			if(pointsNum / sampleNum >= minSampleInterval) break;
			-- sampleNum;
		}
		// sample
		if(sampleNum > 0) {
			Long samples[] = new Long[sampleNum];
			int sampleInterval = pointsNum / sampleNum;
			int downRoundedPointsNum = sampleInterval * sampleNum;
			int rounded = pointsNum - downRoundedPointsNum;
			for(int i=1; i<=sampleNum; ++i) {
					int idx = rounded + i * sampleInterval - 1;
					samples[i-1] = timeArray[idx];
			}
			return samples;
		}
		return null;
	}

	/**
	 *  ZCTODO take care of the unit convert problem while running on non-modified Hadoop.
	 * @return
	 */
	public Map<Long, Long> getPoints() {
		TreeMap<Long, Long> points = new TreeMap<>();		
		long formerRelativeTime = 0l;
		sort();
		for(MemUsage usage : memUsageRecords) {
			// take care of the relative and absolute time problem!
			if (earliestTime == Long.MAX_VALUE) earliestTime = usage.getTimestampMilli();
			// take care of the "unknown" record
			long relativeTime = getRelativeTime(usage.getTimestampMilli());
			if(relativeTime >= formerRelativeTime && !usage.getMemUsed().equals("unknown")) {
				points.put(relativeTime, Long.parseLong(usage.getMemUsed()));
				formerRelativeTime = relativeTime;
			}
		}
		memUsages = points;
		return points;
	}
	
	private long getRelativeTime(long absTime) {
		if(absTime - earliestTime < 0) System.err.println("******Error in sorting memory usage record!");
		return absTime - earliestTime;
	}
	
	public long getAbsTime(long relativeTime) {
		return earliestTime + relativeTime;
	}

	/* canceled by ZC at 20141229
	private void fillUnknownMem() {
		int size = this.memUsageRecords.size();
		if (size < 3) {
			System.err.println("Memory useage information records too few!");
			return;
		}
		for (int i = 0; i < size; ++i) {
			MemUsage u = this.memUsageRecords.get(i);
			String memTotal = u.getMemTotal();
			if (memTotal.equals("unknown")) {
				if (i + 1 < size) {
					u.setMemTotal(this.memUsageRecords.get(i + 1).getMemTotal());
					break;
				}
			}
		}
		for (int i = size - 1; i >= 0; --i) {
			MemUsage u = this.memUsageRecords.get(i);
			String memTotal = u.getMemTotal();
			if (memTotal.equals("unknown")) {
				if (i - 1 >= 0) {
					u.setMemTotal(this.memUsageRecords.get(i - 1).getMemTotal());
					break;
				}
			}
		}
		//////////////////// fill jvm mem usage ////////////////////
		int size2 = this.jvmUsageRecords.size();
		if (size2 < 3) {
			System.err.println("Jvm memory useage information records too few!");
			return;
		}
		for (int i = 0; i < size2; ++i) {
			JvmMemUsage ju = this.jvmUsageRecords.get(i);
			String memTotal = ju.getMemTotal();
			if (memTotal.equals("unknown")) {
				if (i + 1 < size2) {
					ju.setMemTotal(this.jvmUsageRecords.get(i + 1).getMemTotal());
					break;
				}
			}
		}
		for (int i = size2 - 1; i >= 0; --i) {
			JvmMemUsage ju = this.jvmUsageRecords.get(i);
			String memTotal = ju.getMemTotal();
			if (memTotal.equals("unknown")) {
				if (i - 1 >= 0) {
					ju.setMemTotal(this.jvmUsageRecords.get(i - 1).getMemTotal());
					break;
				}
			}
		}
	}
	*/
/*
	public long getTimeSpan() {
		long maxtime = 0, mintime = Long.MAX_VALUE;
		for (MemUsage usage : memUsageRecords) {
			long curTime = usage.getTimestampMilli();
			if (curTime < mintime)
				mintime = curTime;
			if (curTime > maxtime)
				maxtime = curTime;
		}
		return maxtime - mintime;
	}
*/
	public void sort() {
		Collections.sort(this.memUsageRecords);
		Collections.sort(this.jvmUsageRecords);
		Collections.sort(this.gmarks);
		//fillUnknownMem(); // canceled by ZC at 20141229
	}

	@Override
	public String toString() {
		String s = "taskContainerId: " + this.taskContainerId + "\n"
				+ "processTreeId: " + this.processTreeId + "\n";
		String s2 = "";
		sort();
		s2 += "os mem usage: \n";
		for (MemUsage record : this.memUsageRecords) {
			s2 += record.toString() + "\n";
		}
		s2 += "jvm mem usage: \n";
		for (JvmMemUsage record : this.jvmUsageRecords) {
			s2 += record.toString() + "\n";
		}
		s2 += "gmarks: \n";
		for (GMark record : this.gmarks) {
			s2 += record.toString() +"\n";
		}
		// String s4 = "" + getTimeSpan();
		return s + s2 + "\n";
	}
}
