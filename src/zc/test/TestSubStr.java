package zc.test;

public class TestSubStr {

	public static void main(String[] args) {
		String NMLogName = "/home/zc/ZCB" + "/" + "centos2432" + "/yarn/yarn-" + "zc"
				+ "-nodemanager-" + "centos324324" + ".log";
		int start = NMLogName.lastIndexOf("-nodemanager-") + "-nodemanager-".length();
		int end = NMLogName.indexOf(".log", start);
		System.out.println(NMLogName.substring(start, end));
	}

}
