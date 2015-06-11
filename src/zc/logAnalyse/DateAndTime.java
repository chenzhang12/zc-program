package zc.logAnalyse;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateAndTime {

	private static String pattern1 = "\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\,\\d{3}";
	private static String pattern2 = "\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}";
	private static String pattern3 = "\\d{4}-\\d{2}-\\d{2}";
	private static String pattern4 = "\\d+";
	public static String NOW = "now";
	public static SimpleDateFormat sdf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss,SSS");

	public static long dateToTime(String dateStr) {

		if (dateStr.matches(pattern1)) {
			// do nothing because it is the full format.
		} else if (dateStr.matches(pattern4)) {
			return Long.parseLong(dateStr);
		} else if (dateStr.matches(pattern3)) {
			dateStr = dateStr + " 00:00:00,000";
		} else if (dateStr.matches(pattern2)) {
			dateStr = dateStr + ",000";
		} else if (dateStr.equalsIgnoreCase(NOW)) {
			return System.currentTimeMillis();
		} else {
			return -1; // invalid data format
		}

		Date dt = null;
		try {
			dt = sdf.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		long time = dt.getTime();
		return time;
	}

	public static String timeToDate(String timeStr) {
		if (timeStr.matches(pattern4)) {
			long time = Long.valueOf(timeStr);
			return timeToDate(time);
		}
		return null;
	}

	public static String timeToDate(long time) {
		Date dt = new Date(time);
		return sdf.format(dt);
	}
	
	public static long roundDownMills(long time) {
		return time / 1000 * 1000;
	}

	// for test
	public static void main(String[] args) {
		long time = DateAndTime.dateToTime("2014-01-21 17:36:38,692");
		System.out.println(time);
		System.out.println(DateAndTime.timeToDate(time));
		System.out.println(DateAndTime.timeToDate(-1l));
		System.out.println(DateAndTime.timeToDate(System.currentTimeMillis()));
	}
}
