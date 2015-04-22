package zc.logAnalyse;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TaskAttempt implements Comparable<TaskAttempt>, ResultOutputable {

	private String taskAttemptId = null; // group(2);
	private String taskContainerId = null; // group(5);
	private String nodeId = null; // group(6);
	private String appattemptId = null; // application attempt id
	private String type = null; // group(4);
	//private String timestamp = null; // group(1);
	private long timestampMilli = 0L;
	
	// ZCTODO to be serialized///////////////////////////////////////
	private long successfulFinishTime = Long.MAX_VALUE;
	private long startTime = -1l;
	/////////////////////////////////////////////////////////////////

	TaskContainer taskContainer = null;

	public TaskAttempt(String taskAttemptId, String taskContainerId, String nodeId,
			String appattemptId, String taskType, String timestamp) {
		this.taskAttemptId = taskAttemptId;
		this.taskContainerId = taskContainerId;
		this.nodeId = nodeId;
		this.appattemptId = appattemptId;
		this.type = taskType;
		//this.timestamp = timestamp;
		this.timestampMilli = DateAndTime.dateToTime(timestamp);		
		this.taskContainer = new TaskContainer(this.taskContainerId, LogAnalyser.conf);
	}

	public String getTaskContainerId() {
		return taskContainerId;
	}

	public String getTaskAttemptId() {
		return taskAttemptId;
	}

	public void setTaskContainerId(String taskContainerId) {
		this.taskContainerId = taskContainerId;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getAppattemptId() {
		return appattemptId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getTimestamp() {
		return DateAndTime.timeToDate(timestampMilli);
	}

	public long getTimestampMilli() {
		return timestampMilli;
	}

	public void setTaskAttemptId(String taskAttemptId) {
		this.taskAttemptId = taskAttemptId;
	}

	public TaskContainer getTaskContainer() {
		return taskContainer;
	}
	

	public long getFinishTime() {
		return successfulFinishTime;
	}

	public void setFinishTime(String successfulFinishTimeStr) {
		this.successfulFinishTime = DateAndTime.dateToTime(successfulFinishTimeStr);
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTimeStr) {
		this.startTime = DateAndTime.dateToTime(startTimeStr);
	}
	
	public boolean timeValid() {
		return startTime > 0 && successfulFinishTime < Long.MAX_VALUE;
	}

	@Override
	public int compareTo(TaskAttempt that) {
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
		String s = "taskAttemptId: " + this.taskAttemptId + "\n"
				 + "taskContainerId: " + this.taskContainerId + "\n"
				 + "nodeId: " + this.nodeId + "\n"
				 + "appattemptId: " + this.appattemptId + "\n"
				 + "type: " + this.type + "\n"
				 + "timestamp: " + this.getTimestamp() + "\n"
				 + "timestampMilli: " + this.timestampMilli + "\n";
		String s2 = taskContainer.toString();
		return s + "\n" + s2;
	}

	@Override
	public void outputResult(String fatherDirName) {
		this.taskContainer.sort();
		String taskInfoFile = fatherDirName + "/" + this.taskAttemptId;
		try {
			DataOutputStream dout = new DataOutputStream(new FileOutputStream(taskInfoFile));
			dout.writeUTF(this.getTaskAttemptId());
			String unit = "MB";
			dout.writeUTF(unit);
			dout.writeInt(taskContainer.getUsageRecords().size());				
			for(MemUsage usage : taskContainer.getUsageRecords()) {
				dout.writeLong(usage.getTimestampMilli());
				dout.writeDouble(UnitConverter.convert(usage.getMemUsed(), unit));
				dout.writeDouble(UnitConverter.convert(usage.getMemTotal(), unit));
			}
			List<Map<Long,Long>> sampledCurves = taskContainer.getSampledCurves();
			dout.writeInt(sampledCurves.size());
			for(Map<Long, Long> curve : sampledCurves) {
				dout.writeInt(curve.size());
				for(Map.Entry<Long, Long> point : curve.entrySet()) {
					dout.writeLong(point.getKey());
					dout.writeDouble(UnitConverter.BtoMB(point.getValue()));
				}
			}
			/* write jvm mem info
			dout.writeInt(taskContainer.getJvmUsageRecords().size());
			for(JvmMemUsage jmu : taskContainer.getJvmUsageRecords()) {
				dout.writeLong(jmu.getTimestampMilli());
				dout.writeDouble(UnitConverter.convert(jmu.getMemUsed(), unit));
				dout.writeDouble(UnitConverter.convert(jmu.getMemTotal(), unit));
			}
			// write gmarks
			dout.writeInt(taskContainer.getGMarks().size());
			for(GMark gm : taskContainer.getGMarks()) {
				dout.writeLong(gm.getTimestampMilli());
				dout.writeUTF(gm.getType());
				dout.writeInt(gm.getContent());
			}
			*/
			dout.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
