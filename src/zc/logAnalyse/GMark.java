package zc.logAnalyse;

public class GMark implements Comparable<GMark> {
	
	public static String mapComplete = "M_OUTPUTS";
	public static String copyPhaseEnd = "CP_END";
	public static String sortPhaseStart = "ST_START";
	public static String finalMergeStart = "FM_START";
	public static String sortPhaseEnd = "ST_END";
	
	private long timestampMilli = 0L;
	private String type = null; // M_OUTPUTS,  CP_END
	private int content = -1;
	
	public GMark(String timestamp, String type, int content) {
		this.timestampMilli = DateAndTime.dateToTime(timestamp);
		this.type = type;
		this.content = content;
	}

	public long getTimestampMilli() {
		return timestampMilli;
	}

	public String getType() {
		return type;
	}

	public int getContent() {
		return content;
	}
	
	@Override
	public int compareTo(GMark that) {
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
		String s = "< " + this.timestampMilli + ", " + this.type + ", " + this.content+ " >";
		return s;
	}
}
