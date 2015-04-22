package zc.test;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import static zc.logAnalyse.ZCDatasets.*;


public class TestParseDatasetConf {
	
	public static void init(String confFile) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(confFile));
			String confLine = null;
			System.out.println("start to parse...");
			while((confLine = br.readLine()) != null) {
				System.out.println(confLine);
				confLine = confLine.trim();
				if(confLine.startsWith("#") || confLine.equals("")) continue;
				String[] variableAndValues = confLine.split("=");
				if(variableAndValues.length < 2) continue;
				String variable = variableAndValues[0].trim();
				String values = variableAndValues[1].trim();
				String[] parsedValues = parseVal(values);
				switch (variable) {
				case "reduce.number.group":
					ParaRanges.reduceNumGroups = new int[parsedValues.length];
					for(int i=0; i<parsedValues.length; ++i) {
						String valStr = parsedValues[i];
						boolean isDefault = false;
						if(valStr.startsWith("*")) {
							valStr = valStr.substring(1).trim();
							isDefault = true;
						}
						int val = Integer.parseInt(valStr);
						ParaRanges.reduceNumGroups[i] = val;
						if(isDefault) DefaultParas.reduceNumGroup = val;
					}
					break;
				case "mapred.request.memory.group":
					ParaRanges.MRReqGRoups = new int[parsedValues.length];
					for(int i=0; i<parsedValues.length; ++i) {
						String valStr = parsedValues[i];
						boolean isDefault = false;
						if(valStr.startsWith("*")) {
							valStr = valStr.substring(1).trim();
							isDefault = true;
						}
						int val = Integer.parseInt(valStr);
						ParaRanges.MRReqGRoups[i] = val;
						if(isDefault) DefaultParas.MRReqGRoup = val;
					}
					break;
				case "map.request.memory-in-each-group":
					ParaRanges.mapReqs = new int[parsedValues.length];
					for(int i=0; i<parsedValues.length; ++i) {
						String valStr = parsedValues[i];
						boolean isDefault = false;
						if(valStr.startsWith("*")) {
							valStr = valStr.substring(1).trim();
							isDefault = true;
						}
						int val = Integer.parseInt(valStr);
						ParaRanges.mapReqs[i] = val;
						if(isDefault) DefaultParas.mapReq = val;
					}
					break;
				case "reduce.request.memory-in-each-group":
					ParaRanges.reduceReqs = new int[parsedValues.length];
					for(int i=0; i<parsedValues.length; ++i) {
						String valStr = parsedValues[i];
						boolean isDefault = false;
						if(valStr.startsWith("*")) {
							valStr = valStr.substring(1).trim();
							isDefault = true;
						}
						int val = Integer.parseInt(valStr);
						ParaRanges.reduceReqs[i] = val;
						if(isDefault) DefaultParas.reduceReq = val;
					}
					break;
				case "predra.delta":
					ParaRanges.deltas = new float[parsedValues.length];
					for(int i=0; i<parsedValues.length; ++i) {
						String valStr = parsedValues[i];
						boolean isDefault = false;
						if(valStr.startsWith("*")) {
							valStr = valStr.substring(1).trim();
							isDefault = true;
						}
						float val = Float.parseFloat(valStr);
						ParaRanges.deltas[i] = val;
						if(isDefault) DefaultParas.delta = val;
					}
					break;
				case "predra.beta":
					ParaRanges.betas = new float[parsedValues.length];
					for(int i=0; i<parsedValues.length; ++i) {
						String valStr = parsedValues[i];
						boolean isDefault = false;
						if(valStr.startsWith("*")) {
							valStr = valStr.substring(1).trim();
							isDefault = true;
						}
						float val = Float.parseFloat(valStr);
						ParaRanges.betas[i] = val;
						if(isDefault) DefaultParas.beta = val;
					}
					break;
				case "predra.gamma":
					ParaRanges.gammas = new float[parsedValues.length];
					for(int i=0; i<parsedValues.length; ++i) {
						String valStr = parsedValues[i];
						boolean isDefault = false;
						if(valStr.startsWith("*")) {
							valStr = valStr.substring(1).trim();
							isDefault = true;
						}
						float val = Float.parseFloat(valStr);
						ParaRanges.gammas[i] = val;
						if(isDefault) DefaultParas.gamma = val;
					}
					break;
				case "predra.sigma":
					ParaRanges.sigmas = new float[parsedValues.length];
					for(int i=0; i<parsedValues.length; ++i) {
						String valStr = parsedValues[i];
						boolean isDefault = false;
						if(valStr.startsWith("*")) {
							valStr = valStr.substring(1).trim();
							isDefault = true;
						}
						float val = Float.parseFloat(valStr);
						ParaRanges.sigmas[i] = val;
						if(isDefault) DefaultParas.sigma = val;
					}
					break;
				default:
					System.err.println("*Unsupport variable: " + variable);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(br != null)
				try {
					br.close();
				} catch (IOException e) {e.printStackTrace();}
		}
	}
	
	private static String[] parseVal(String values) {
		int s, e;
		String valStr = null;
		String[] ret = null;
		s = values.indexOf("{");
		e = values.indexOf("}");
		if(s > -1 && e > s) {
			valStr = values.substring(s+1, e);
		}
		if(valStr != null) {
			ret = valStr.split(",");
			if(ret != null) {
				for(int i=0; i<ret.length; ++i) {
					ret[i] = ret[i].trim();
				}
			}
		}
		return ret;
	}

	public static void main(String[] args) {
		init("/home/zc/ZCBenchmarkSuite/etc/parameters-display.conf");
		printArray(ParaRanges.reduceNumGroups);
		println(DefaultParas.reduceNumGroup);
		printArray(ParaRanges.MRReqGRoups);
		println(DefaultParas.MRReqGRoup);
		printArray(ParaRanges.mapReqs);
		println(DefaultParas.mapReq);
		printArray(ParaRanges.reduceReqs);
		println(DefaultParas.reduceReq);
		printArray(ParaRanges.deltas);
		println(DefaultParas.delta);
		printArray(ParaRanges.betas);
		println(DefaultParas.beta);
		printArray(ParaRanges.gammas);
		println(DefaultParas.gamma);
		printArray(ParaRanges.sigmas);
		println(DefaultParas.sigma);

	}
	
	public static void println(Object obj) {
		System.out.println(obj);
	}
	
	public static void printArray(Object[] array) {
		System.out.print("{");
		boolean isFirst = true;
		for(Object o : array) {
			if(isFirst) { System.out.print(o); isFirst = false;}
			else {
				System.out.print(", " + o);
			}
		}
		System.out.println("}");
	}
	
	public static void printArray(int[] array) {
		System.out.print("{");
		boolean isFirst = true;
		for(int o : array) {
			if(isFirst) { System.out.print(o); isFirst = false;}
			else {
				System.out.print(", " + o);
			}
		}
		System.out.println("}");
	}
	
	public static void printArray(float[] array) {
		System.out.print("{");
		boolean isFirst = true;
		for(float o : array) {
			if(isFirst) { System.out.print(o); isFirst = false;}
			else {
				System.out.print(", " + o);
			}
		}
		System.out.println("}");
	}
}
