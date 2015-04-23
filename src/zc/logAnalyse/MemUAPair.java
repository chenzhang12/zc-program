package zc.logAnalyse;

public class MemUAPair extends Pair<Long, Long> implements Comparable<MemUAPair>{
	
	private long time;

	public MemUAPair(long time, long memUsed, long memAlloc) {
		super(memUsed, memAlloc);
		this.time = time;
	}
	
	public long getTimeAbs() {
		return time;
	}
	
	public long getMemUsed() {
		return getKey();
	}
	
	public long getMemAlloc() {
		return getValue();
	}

	@Override
	public int compareTo(MemUAPair that) {
		long diff = this.time - that.time;
		if(diff > 0) return 1;
		if(diff < 0) return -1;
		return 0;
	}
}
