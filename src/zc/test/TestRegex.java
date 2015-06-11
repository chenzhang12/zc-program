package zc.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import zc.logAnalyse.AppAttempt;
import static zc.logAnalyse.ZCPatterns.*;

public class TestRegex {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String pstr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO org\\.apache\\.hadoop\\.yarn\\.server\\.nodemanager\\.containermanager\\.ContainerManagerImpl: Start request for (container_\\d+_\\d+_\\d+_\\d+) by user zc";
		String pstr1 = "\\[(appattempt_.+)\\]\\[submit time: (.+)\\]\\[start time: (.+)\\]\\[finish time: (.+)\\]";
		Pattern p2 = Pattern.compile(pstr);
		Pattern p = cReqStartPattern;
		String str = "2015-05-27 20:16:57,816 INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl: Start request for container_1432757784415_0002_01_000127 by user zc";
		String str1 = "2015-05-27 20:16:57,816 INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl: Start request for container_1432757784415_0002_01_000127 by user zc";
		String str2 = "2014-11-09 12:20:53,621 INFO [AsyncDispatcher event handler] org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl: attempt_1415504548970_0007_m_000010_0 TaskAttempt Transitioned from SUCCESS_CONTAINER_CLEANUP to SUCCEEDED";
		Matcher m = p.matcher(str + str2 + str1);
		
		while(m.find()) {
			for(int i=0; i<=m.groupCount(); ++i) {
				System.out.println("group("+ i +"): "+ m.group(i));
			}
		}
	}
}
