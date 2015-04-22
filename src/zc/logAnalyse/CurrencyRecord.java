package zc.logAnalyse;

public class CurrencyRecord implements Comparable<MemUsage> {
	
	private long timestampMilli = 0L;
	private int tasksRunning;

	
	public CurrencyRecord(String timestamp, int tasks) {
		this.timestampMilli = DateAndTime.dateToTime(timestamp);
		this.tasksRunning = tasks;
	}

	public String getTimestamp() {
		return DateAndTime.timeToDate(this.timestampMilli);
	}

	public long getTimestampMilli() {
		return timestampMilli;
	}

	public int getTasksRunning() {
		return tasksRunning;
	}

	@Override
	public int compareTo(MemUsage that) {
		long t = this.timestampMilli - that.getTimestampMilli();
		if(t < 0) {
			return -1;
		} else if (t > 0) {
			return 1;
		} else {
			return 0;
		}		
	}
	
}
