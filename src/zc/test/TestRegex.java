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
		String pstr = "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}) INFO org\\.apache\\.hadoop\\.yarn\\.server\\.nodemanager\\.containermanager\\.monitor\\.ContainersMonitorImpl: Number of running containers on this node is (\\d+)";
		String pstr1 = "\\[(appattempt_.+)\\]\\[submit time: (.+)\\]\\[start time: (.+)\\]\\[finish time: (.+)\\]";
		Pattern p2 = Pattern.compile(pstr1);
		Pattern p = taskAttemptSuccessPattern;
		String str = "2014-11-09 12:21:16,206 INFO [AsyncDispatcher event handler] org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl: attempt_1415504548970_0007_m_000016_0 TaskAttempt Transitioned from SUCCESS_CONTAINER_CLEANUP to SUCCEEDED";
		String str1 = "[appattempt_1425705353847_0087_000001][submit time: 2015-03-07 05:37:25,257][start time: 2015-03-07 07:08:23,945][finish time: 2015-03-07 07:11:02,655]";
		String str2 = "2014-11-09 12:20:53,621 INFO [AsyncDispatcher event handler] org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl: attempt_1415504548970_0007_m_000010_0 TaskAttempt Transitioned from SUCCESS_CONTAINER_CLEANUP to SUCCEEDED";
		Matcher m = p2.matcher(str + str2 + str1);
		
		while(m.find()) {
			for(int i=0; i<=m.groupCount(); ++i) {
				System.out.println("group("+ i +"): "+ m.group(i));
			}
		}
	}
}
