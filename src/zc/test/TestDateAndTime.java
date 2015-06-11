package zc.test;

import zc.logAnalyse.DateAndTime;

public class TestDateAndTime {

	public static void main(String[] args) {
		long time = System.currentTimeMillis();
		long roundTime = DateAndTime.roundDownMills(time);
		System.out.println(time);
		System.out.println(roundTime);
	}

}
