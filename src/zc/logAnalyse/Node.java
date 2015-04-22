package zc.logAnalyse;

import java.util.Set;
import java.util.TreeSet;

public class Node implements Comparable<Node>, ResultOutputable{
	
	private String nodeId;
	private Set<CurrencyRecord> currencyRecords;
	
	public Node(String nodeId) {
		this.nodeId = nodeId;
		currencyRecords = new TreeSet<CurrencyRecord>();
	}

	@Override
	public void outputResult(String fatherDirName) {
		String appDir = fatherDirName + "/" + this.nodeId;
		
	}

	@Override
	public int compareTo(Node that) {
		this.nodeId.compareTo(that.nodeId);
		return 0;
	}

}
