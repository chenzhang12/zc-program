package zc.logAnalyse;

public class JvmMemUsage implements Comparable<JvmMemUsage>{
	
	private long timestampMilli = 0L;
	private String memUsed = null;
	private String memTotal = null;
	private long memUsedB;

	
	public JvmMemUsage(String timestamp, String memUsed, String memTotal) {
		this.timestampMilli = DateAndTime.dateToTime(timestamp);
		this.memUsed = memUsed;
		this.memTotal = memTotal;
	}
	
	public JvmMemUsage(long timestampMilli, String memUsed, String memTotal) {
		this.timestampMilli = timestampMilli;
		this.memUsed = memUsed;
		this.memTotal = memTotal;
	}

	public String getTimestamp() {
		return DateAndTime.timeToDate(this.timestampMilli);
	}

	public long getTimestampMilli() {
		return timestampMilli;
	}

	public String getMemUsed() {
		return memUsed;
	}

	public String getMemTotal() {
		return memTotal;
	}
	
	public void setMemTotal(String memTotal) {
		this.memTotal = memTotal;
	}

	@Override
	public int compareTo(JvmMemUsage that) {
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
		String s = "< " + this.timestampMilli + ", " + this.memUsed + ", " + this.memTotal+ " >";
		return s;
	}
}
