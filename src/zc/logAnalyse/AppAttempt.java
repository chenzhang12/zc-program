package zc.logAnalyse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AppAttempt implements Comparable<AppAttempt>, ResultOutputable{
	
	private String appAttemptId = null; // group(6);
	private String appId = null; // "application_" + group(7);
	private String appContainerId = null; // group(2);
	private String appContainerResource = null; // group(4);
	private String nodeId = null; // group(3);
	private String priority = null; // group(5);
	//private String timestamp = null; // group(1);
	private long timestampMilli = 0L;

	private Map<String, TaskAttempt> tidTotasks = null;
	private Map<String, TaskContainer> cidToTaskContainer = null;
	
	///////////// ZCTODO: added at 2014-11-27. to be serialized /////////////
	private long submittedTime = -1l; // absolute time
	private long startRunTime = -1l; // absolute time
	private long finishTime = -1l; // absolute time
	
	/////////////////////////////////////////////////////////////////////////
	
	public AppAttempt(String appAttemptId, String appId, String appContainerId,
			String appContainerResource, String nodeId, String priority, String timestamp) {
		this.appAttemptId = appAttemptId;
		this.appId = "application_" + appId;
		this.appContainerId = appContainerId;
		this.appContainerResource = appContainerResource;
		this.nodeId = nodeId;
		this.priority = priority;
		//this.timestamp = group1;
		this.timestampMilli = DateAndTime.dateToTime(timestamp);

		tidTotasks = new HashMap<String, TaskAttempt>();
		cidToTaskContainer = new HashMap<String, TaskContainer>();
	}
	
	public TaskAttempt getTaskAttemptById(String tid) {
		return this.tidTotasks.get(tid);
	}
	
	public void addTaskAttempt(TaskAttempt taskAttempt) {
		this.tidTotasks.put(taskAttempt.getTaskAttemptId(), taskAttempt);
		this.cidToTaskContainer.put(taskAttempt.getTaskContainerId(), taskAttempt.getTaskContainer());
	}
	
	public TaskContainer getTaskContainerById(String cid) {
		return this.cidToTaskContainer.get(cid);
	}

	public String getAppAttemptId() {
		return appAttemptId;
	}

	public String getAppId() {
		return appId;
	}

	public String getAppContainerId() {
		return appContainerId;
	}

	public String getAppContainerResource() {
		return appContainerResource;
	}

	public String getNodeId() {
		return nodeId;
	}

	public String getPriority() {
		return priority;
	}

	public String getTimestamp() {
		return DateAndTime.timeToDate(timestampMilli);
	}

	public long getTimestampMilli() {  
		return timestampMilli;
	}
	
	public boolean timesValid() {
		return startRunTime > 0 && submittedTime > 0 && finishTime > 0;
	}

	public long getStartRunTime() {
		return startRunTime;
	}

	public void setStartRunTime(String startRunTimeStr) {
		this.startRunTime = DateAndTime.dateToTime(startRunTimeStr);
	}

	public long getSubmittedTime() {
		return submittedTime;
	}

	public void setSubmittedTime(String submittedTimeStr) {
		this.submittedTime = DateAndTime.dateToTime(submittedTimeStr);
	}
	
	public void setFinishTime(String finishTime) {
		this.finishTime = DateAndTime.dateToTime(finishTime);
	}
	
	public long getFinishTime() {
		return finishTime;
	}

	@Override
	public int compareTo(AppAttempt that) {
		long t = this.timestampMilli - that.getTimestampMilli();
		if(t < 0) {
			return -1;
		} else if (t > 0) {
			return 1;
		} else {
			return 0;
		}
	}
	
	@Override
	public String toString() {
		String s = "appAttemptId: " + this.appAttemptId + "\n"
				 + "appId: " + this.appId + "\n"
				 + "appContainerId: " + this.appContainerId + "\n"
				 + "appContainerResource: " + this.appContainerResource + "\n"
				 + "nodeId: " + this.nodeId + "\n"
				 + "priority: " + this.priority + "\n"
				 + "reordTimestamp: " + this.getTimestamp() + "\n"
				 + "timestampMilli: " + this.timestampMilli + "\n";
		String s2 = "";
		for(Map.Entry<String, TaskAttempt> entry : tidTotasks.entrySet()) {
			s2 += entry.getKey() + ": \n" + entry.getValue().toString() + "\n";
		}
		
		return s + "\n" + s2;
	}

	@Override
	public void outputResult(String fatherDirName) {
		String appDir = fatherDirName + "/" + this.appAttemptId;
		File f = new File(appDir);
		if(!f.exists()) f.mkdir();
		String appInfoFile = appDir + "/appInfo";
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(appInfoFile));
			bw.write(this.formatResult());
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(Map.Entry<String, TaskAttempt> entry : tidTotasks.entrySet()) {
			entry.getValue().outputResult(appDir);
		}
	}

	public String formatResult() {
		String s = "appAttemptId: " + this.appAttemptId + "\n"
				 + "appId: " + this.appId + "\n"
				 + "appContainerId: " + this.appContainerId + "\n"
				 + "appContainerResource: " + this.appContainerResource + "\n"
				 + "nodeId: " + this.nodeId + "\n"
				 + "priority: " + this.priority + "\n"
				 + "reordTimestamp: " + this.getTimestamp() + "\n"
				 + "timestampMilli: " + this.timestampMilli + "\n";
		return s;
	}	
	
}
