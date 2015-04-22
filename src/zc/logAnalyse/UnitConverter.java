package zc.logAnalyse;

public class UnitConverter {
		
	/**
	 * @param size: "xx.x KB,MB,GB,TB"
	 * @return "xxx.xx"
	 */
	public static double toKB(String numStr) {
		
		String[] s = numStr.split(" ");
		double num = Double.parseDouble(s[0].trim());
		double result;
		switch(s[1].trim()) {
		case "TB" :
			result = num * 1073741824;
			break;
		case "GB" :
			result = num * 1048576;
			break;
		case "MB" :
			result = num * 1024;
			break;
		case "KB" :
			result = num;
			break;
		case "unknown" :
			result = 0;
			break;
		default:
			System.err.println("Illegal unit: " + s[1]);
			return -1;
		}
		return result;
	}
	
	/**
	 * @param size: "xx.x MB,GB,TB"
	 * @return "xxx.xx"
	 */
	public static double toMB(String numStr) {
		
		String[] s = numStr.split(" ");
		double num = 0.0;
		String numPart = s[0].trim();
		try {
			num = Double.parseDouble(numPart);
		} catch (NumberFormatException e) {
			if(numPart.equals("unknown")) return 0.0;
			else {
				e.printStackTrace();
				return -1;
			}
		}
		double result = -1;
		String fromUnit = s.length > 1 ? s[1].trim() : "B";
		switch(fromUnit) {
		case "TB" :
			result = num * 1048576;
			break;
		case "GB" :
			result = num * 1024;
			break;
		case "MB" :
			result = num;
			break;
		case "KB" :
			result = num / 1024;
			break;
		case "B" :
			result = num / 1024 / 1024;
			break;
		default:
			System.err.println("Illegal unit: " + s[1]);
			return -1;
		}
		return result;
	}
	
	public static double convert(String originStr, String targetUnit) {
		switch(targetUnit.trim()) {
		case "MB":
			return toMB(originStr);
		case "KB":
			return toKB(originStr);
		default:
			System.err.println("unsupported unit!");
			return -1;
		}
	}
	
	public static double BtoMB(long memB) {
		return memB / 1024 /1024.0d;
	}
	
	public static String toB(String numStr) {
		
		String[] s = numStr.split(" ");
		float num = 0.0f;
		String numPart = s[0].trim();
		try {
			num = Float.parseFloat(numPart);
		} catch (NumberFormatException e) {
			if(numPart.equals("unknown")) return null;
			else {
				e.printStackTrace();
				return null;
			}
		}
		long result = -1;
		String fromUnit = s.length > 1 ? s[1].trim() : "B";
		switch(fromUnit) {
		case "TB" :
			result = (long)(num * 1099511627776l);
			break;
		case "GB" :
			result = (long)(num * 1073741824);
			break;
		case "MB" :
			result = (long)(num * 1048576);
			break;
		case "KB" :
			result = (long)(num * 1024);
			break;
		case "B" :
			result = (long)num;
			break;
		default:
			System.err.println("Illegal unit: " + s[1]);
			return null;
		}
		return Long.toString(result);
	}
	
	/*
	public static void main(String[] a) {
		String str1 = "1 GB";
		System.out.println(Double.parseDouble(UnitConverter.toB(str1)) / 1073741824);
	}
	*/
}
