package zc.logAnalyse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import static zc.logAnalyse.ZCPatterns.*;
import static zc.logAnalyse.DateAndTime.dateToTime;

public class ZCDatasets {

	HashMap<String, String> metrics = null;
	HashMap<String, TreeMap<Integer,Float>> CDFs = null;
	HashMap<String, TreeMap<Long, MemUAPair>> uaSeriesMap = null;
	HashMap<String, TreeMap<Long, UAScTuple>> uascsMap = null;
	HashMap<String, TreeSet<AppAttemptData>> appDatasets = null;
	String rowNameOfCDF = null;
	String colNameOfCDF = null;
	
	public static boolean inited = false;
	public static final double CHART_ERROR = 0.0;
	public static final double DATA_ERROR = -1000.0;
	
	// defaults
	public static class DefaultParas {
		public static int reduceNumGroup = 3;
		public static int MRReqGRoup = 3;
		public static int mapReq = 2048;
		public static int reduceReq = 4096;
		public static float delta = 0.1f;
		public static float beta = 1.6f;
		public static float gamma = 1.8f;
		public static float sigma = 0.5f;
	}
	
	public static class ParaRanges {
		public static int[] reduceNumGroups = new int[]{1,2,3,4,5};
		public static int[] MRReqGRoups = new int[]{1,2,3,4,5};
		public static int[] mapReqs = new int[]{1024, 1536, 2048, 2560, 3072};
		public static int[] reduceReqs = new int[]{2048, 3072, 4096, 5120, 6144};
		public static float[] deltas = new float[]{0.1f, 0.2f, 0.3f, 0.4f, 0.5f};
		public static float[] betas = new float[]{1.2f, 1.4f, 1.6f, 1.8f, 2.0f};
		public static float[] gammas = new float[]{1.4f, 1.8f, 2.2f, 2.6f, 3.0f};
		public static float[] sigmas = new float[]{0.1f, 0.3f, 0.5f, 0.7f, 0.9f};
	}
	
	public static class AppAttemptData implements Comparable<AppAttemptData> {
		public String aaid;
		public long submitTime;
		public long startTime;
		public long finishTime;
		public AppAttemptData(String aaid, long submitTime, long startTime, long finishTime) {
			this.aaid = aaid;
			this.submitTime = submitTime;
			this.startTime = startTime;
			this.finishTime = finishTime;
		}
		public static AppAttemptData newInstance(String lineToParse) {
			Matcher m = zcAResultAppAttemptPattern.matcher(lineToParse); //TODO
			AppAttemptData aad = null;
			if(m.find()) {
				String aaid = m.group(1);
				long submit = dateToTime(m.group(2));
				long start = dateToTime(m.group(3));
				long finish = dateToTime(m.group(4));
				aad = new AppAttemptData(aaid, submit, start, finish);
			}
			return aad;
		}
		@Override
		public int compareTo(AppAttemptData o) {
			long d = this.submitTime - o.submitTime;
			if(d < 0) return -1;
			if(d > 0) return 1;
			return 0;
		}
	}
	
	public static enum WorkLoadType {
		SE, SNS
	}

	public static enum SystemType {
		HADOOP, PREDRA, MROCHESTRATOR, ADMP
	}

	public static enum MetricType {
		TP("TP(perHour)"), AJWT("AJWT"), AJTT("AJTT"), AP("AP"), MUR("MUR"), PPREM(
				"PPREM"), PFR("PFR");
		private String content;

		private MetricType(String content) {
			this.content = content;
		}
		
		public static boolean isTimeMetric(MetricType mt) {
			if(mt == AJTT || mt == AJWT) return true;
			return false;
		}

		@Override
		public String toString() {
			return content;
		}
	}
	
	public static enum ParameterType {
		ReducesGroup, MRReq, Delta, Beta, Gamma, Sigma
	}
	
	public static void init(String confFile) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(confFile));
			String confLine = null;
			while((confLine = br.readLine()) != null) {
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

	public ZCDatasets(String analyseResultDir, String confFile) {
		if(!inited) {
			init(confFile);
			inited = true;
		}
		metrics = new HashMap<>();
		CDFs = new HashMap<>();
		uaSeriesMap = new HashMap<>();
		uascsMap = new HashMap<>();
		appDatasets = new HashMap<>();
		File resultDir = new File(analyseResultDir);
		if (resultDir.isDirectory()) {
			File[] resultFiles = resultDir.listFiles();
			for (File f : resultFiles) {
				if(f.isDirectory()) continue;
				String fileName = f.getName();
				String[] parts = fileName.split("_");
				String usefulParts = parts[0].trim().toUpperCase() + " " + parts[1].trim();
				BufferedReader br = null;
				try {
					br = new BufferedReader(new FileReader(f));
					String line = null;
					String rootKey = null;
					while ((line = br.readLine()) != null) {
						if (line.equals("[title]")) {
							while ((line = br.readLine()) != null) {
								if (!line.trim().equals("")) {
									rootKey = usefulParts + " " + line.trim();
									break;
								}
							}
						} else if (line.equals("[metrics]")) {
							while ((line = br.readLine()) != null) {
								if (line.trim().equals(""))
									break;
								String[] kv = line.trim().split("=");
								String metricKey = kv[0];
								String fullKey = rootKey + " " + metricKey;
								metrics.put(fullKey, kv[1]);
								//System.out.println(fullKey + "=" + kv[1]); // for test
							}
						} else if (line.equals("[CDF]")) {
							line = br.readLine();
							TreeMap<Integer, Float> cdf = new TreeMap<>();
							while ((line = br.readLine()) != null) {
								if (line.trim().equals(""))
									break;
								String[] kv = line.trim().split(" ");
								int diffOfMem = Integer.parseInt(kv[0]);
								float cdfOfDiff = Float.parseFloat(kv[1]);
								cdf.put(diffOfMem, cdfOfDiff);
								//System.out.println(fullKey + "=" + kv[1]); // for test
							}
							CDFs.put(rootKey, cdf);
						} else if (line.equals("[UASeries]")) {
							line = br.readLine();
							TreeMap<Long, MemUAPair> uaSeries = new TreeMap<>();
							while ((line = br.readLine()) != null) {
								if (line.trim().equals(""))
									break;
								String[] tua = line.trim().split(" "); // space
								long time = Long.parseLong(tua[0]);
								long memUsed = Long.parseLong(tua[1]);
								long memAlloc = Long.parseLong(tua[2]);
								uaSeries.put(time, new MemUAPair(time, memUsed, memAlloc));
								//System.out.println(fullKey + "=" + kv[1]); // for test
							}
							uaSeriesMap.put(rootKey, uaSeries);
						} else if (line.equals("[collectedUAScs]")) {
							line = br.readLine();
							TreeMap<Long, UAScTuple> uascs = new TreeMap<>();
							while ((line = br.readLine()) != null) {
								if (line.trim().equals(""))
									break;
								String[] tuas = line.trim().split(" "); // space
								long time = Long.parseLong(tuas[0]);
								long memUsed = Long.parseLong(tuas[1]);
								long memAlloc = Long.parseLong(tuas[2]);
								int taskScheded = Integer.parseInt(tuas[3]);
								uascs.put(time, new UAScTuple(time, memUsed, memAlloc, taskScheded));
								//System.out.println(fullKey + "=" + kv[1]); // for test
							}
							uascsMap.put(rootKey, uascs);
						}
					}
					/////////////// [get appattempt stage time info] ///////////////
					br.close(); 
					br = new BufferedReader(new FileReader(f));
					while ((line = br.readLine()) != null) {
						if(line.startsWith("[appattempt")) {
							// prepare appattempt data in analyzed log to draw
							TreeSet<AppAttemptData> appADataset = appDatasets.get(rootKey);
							if (appADataset == null) {
								appADataset = new TreeSet<>();
								appDatasets.put(rootKey, appADataset);
							}
							appADataset.add(AppAttemptData.newInstance(line));
						}
					}
					/////////////////////////////
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						br.close();
					} catch (IOException e) {
					}
				}
			}
		}
	}

	private String getValue(WorkLoadType wt, int reduceNumGroup, SystemType st, int mapReqMB,
			int reduceReqMB, float delta, float beta, float gamma, float sigma, MetricType metricKey) {
		String key = wt.toString() + " " + reduceNumGroup + " " + st.toString() + " " + mapReqMB + " " + reduceReqMB 
				+ " " + delta + " " + beta + " " + gamma + " " + sigma + " " + metricKey.toString();
		return metrics.get(key);
	}
	
	public Set<AppAttemptData> getAppAStageTimeDatasetOriginal(WorkLoadType wt, int reduceNumGroup, SystemType st, int mapReqMB,
			int reduceReqMB, float delta, float beta, float gamma, float sigma) {
		String key = wt.toString() + " " + reduceNumGroup + " " + st.toString() + " " + mapReqMB + " " + reduceReqMB 
				+ " " + delta + " " + beta + " " + gamma + " " + sigma;
		return appDatasets.get(key);
	}
	
	public HashMap<String, TreeSet<AppAttemptData>> getAppDatasets() {
		return appDatasets;
	}
	
	public DefaultCategoryDataset getAppAStageTimeDataset(TreeSet<AppAttemptData> appaOriginalDataset) {
		if(appaOriginalDataset == null) {
			return null;
		}
		DefaultCategoryDataset paraToMt = new DefaultCategoryDataset();
		if (appaOriginalDataset.size() > 0) {
			long minSubmitTime = appaOriginalDataset.first().submitTime;
			for(AppAttemptData ad : appaOriginalDataset) {
				paraToMt.addValue(ad.submitTime - minSubmitTime, "time wait for submit", ad.aaid);
				paraToMt.addValue(ad.startTime - ad.submitTime, "time wait for start", ad.aaid);
				paraToMt.addValue(ad.finishTime - ad.startTime, "time for running", ad.aaid);
			}
		}
		return paraToMt;
	}
	
	public XYSeries getDeltaSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		XYSeries paraToMt = new XYSeries(mt);
		for(float delta : ParaRanges.deltas) {
			String value = getValue(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, DefaultParas.mapReq,
					DefaultParas.reduceReq, delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double val = value == null ? error : Double.parseDouble(value);
			paraToMt.add(delta, val);
		}
		return paraToMt;
	}
	
	public XYSeries getCompDeltaSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		if(mt == MetricType.PFR || mt == MetricType.PPREM) return null;
		XYSeries paraToMt = new XYSeries(mt);
		for(float delta : ParaRanges.deltas) {
			String predraValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, DefaultParas.mapReq,
					DefaultParas.reduceReq, delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			String hadoopValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.HADOOP, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double predraVal = predraValue == null ? error : Double.parseDouble(predraValue);
			double hadoopVal = hadoopValue == null ? error : Double.parseDouble(hadoopValue);
			if(predraVal <= 0 || hadoopVal <= 0) {
				paraToMt.add(delta, error);
			} else {
				double val = (predraVal - hadoopVal) / hadoopVal;
				if(MetricType.isTimeMetric(mt)) val = -val;
				paraToMt.add(delta, val);
			}
		}
		return paraToMt;
	} 
	
	public XYSeries getBetaSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		XYSeries paraToMt = new XYSeries(mt);
		for(float beta : ParaRanges.betas) {
			String value = getValue(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double val = value == null ? error : Double.parseDouble(value);
			paraToMt.add(beta, val);
		}
		return paraToMt;
	}
	
	public XYSeries getCompBetaSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		if(mt == MetricType.PFR || mt == MetricType.PPREM) return null;
		XYSeries paraToMt = new XYSeries(mt);
		for(float beta : ParaRanges.betas) {
			String predraValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			String hadoopValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.HADOOP, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double predraVal = predraValue == null ? error : Double.parseDouble(predraValue);
			double hadoopVal = hadoopValue == null ? error : Double.parseDouble(hadoopValue);
			if(predraVal <= 0 || hadoopVal <= 0) {
				paraToMt.add(beta, error);
			} else {
				double val = (predraVal - hadoopVal) / hadoopVal;
				if(MetricType.isTimeMetric(mt)) val = -val;
				paraToMt.add(beta, val);
			}
		}
		return paraToMt;
	} 
	
	public XYSeries getGammaSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		XYSeries paraToMt = new XYSeries(mt);
		for(float gamma : ParaRanges.gammas) {
			String value = getValue(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, gamma, DefaultParas.sigma, 
					mt);
			double val = value == null ? error : Double.parseDouble(value);
			paraToMt.add(gamma, val);
		}
		return paraToMt;
	}
	
	public XYSeries getCompGammaSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		if(mt == MetricType.PFR || mt == MetricType.PPREM) return null;
		XYSeries paraToMt = new XYSeries(mt);
		for(float gamma : ParaRanges.gammas) {
			String predraValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, gamma, DefaultParas.sigma, 
					mt);
			String hadoopValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.HADOOP, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double predraVal = predraValue == null ? error : Double.parseDouble(predraValue);
			double hadoopVal = hadoopValue == null ? error : Double.parseDouble(hadoopValue);
			if(predraVal <= 0 || hadoopVal <= 0) {
				paraToMt.add(gamma, error);
			} else {
				double val = (predraVal - hadoopVal) / hadoopVal;
				if(MetricType.isTimeMetric(mt)) val = -val;
				paraToMt.add(gamma, val);
			}
		}
		return paraToMt;
	} 
	
	public XYSeries getSigmaSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		XYSeries paraToMt = new XYSeries(mt);
		for(float sigma : ParaRanges.sigmas) {
			String value = getValue(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, sigma, 
					mt);
			double val = value == null ? error : Double.parseDouble(value);
			paraToMt.add(sigma, val);
		}
		return paraToMt;
	}
	
	public XYSeries getCompSigmaSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		if(mt == MetricType.PFR || mt == MetricType.PPREM) return null;
		XYSeries paraToMt = new XYSeries(mt);
		for(float sigma : ParaRanges.sigmas) {
			String predraValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, sigma, 
					mt);
			String hadoopValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.HADOOP, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double predraVal = predraValue == null ? error : Double.parseDouble(predraValue);
			double hadoopVal = hadoopValue == null ? error : Double.parseDouble(hadoopValue);
			if(predraVal <= 0 || hadoopVal <= 0) {
				paraToMt.add(sigma, error);
			} else {
				double val = (predraVal - hadoopVal) / hadoopVal;
				if(MetricType.isTimeMetric(mt)) val = -val;
				paraToMt.add(sigma, val);
			}
		}
		return paraToMt;
	} 

	public XYSeriesCollection getCollectedPredraCompDataset(WorkLoadType wt, ParameterType pt, MetricType[] mts, boolean forChart) {
		XYSeriesCollection xyseriescollection = new XYSeriesCollection();
		switch(pt) {
		case Delta:
			for(MetricType mt : mts) {
				if(mt == MetricType.PFR || mt == MetricType.PPREM) continue;
				XYSeries deltaToTP = getCompDeltaSeries(wt, mt, forChart);
				xyseriescollection.addSeries(deltaToTP);
			}
			break;
		case Beta:
			for(MetricType mt : mts) {
				if(mt == MetricType.PFR || mt == MetricType.PPREM) continue;
				XYSeries betaToTP = getCompBetaSeries(wt, mt, forChart);
				xyseriescollection.addSeries(betaToTP);
			}
			break;
		case Gamma:
			for(MetricType mt : mts) {
				if(mt == MetricType.PFR || mt == MetricType.PPREM) continue;
				XYSeries gammaToTP = getCompGammaSeries(wt, mt, forChart);
				xyseriescollection.addSeries(gammaToTP);
			}
			break;
		case Sigma:
			for(MetricType mt : mts) {
				if(mt == MetricType.PFR || mt == MetricType.PPREM) continue;
				XYSeries sigmaToTP = getCompSigmaSeries(wt, mt, forChart);
				xyseriescollection.addSeries(sigmaToTP);
			}
			break;
		default:
			System.out.println("We do not try to evaluate parameter "+ pt +" in test for Predra");
			return null;
		}
		return xyseriescollection;
	}
	
	public XYSeriesCollection getCollectedPredraAbsDataset(WorkLoadType wt, ParameterType pt, MetricType[] mts, boolean forChart) {
		XYSeriesCollection xyseriescollection = new XYSeriesCollection();
		switch(pt) {
		case Delta:
			for(MetricType mt : mts) {
				XYSeries deltaToTP = getDeltaSeries(wt, mt, forChart);
				xyseriescollection.addSeries(deltaToTP);
			}
			break;
		case Beta:
			for(MetricType mt : mts) {
				XYSeries betaToTP = getBetaSeries(wt, mt, forChart);
				xyseriescollection.addSeries(betaToTP);
			}
			break;
		case Gamma:
			for(MetricType mt : mts) {
				XYSeries gammaToTP = getGammaSeries(wt, mt, forChart);
				xyseriescollection.addSeries(gammaToTP);
			}
			break;
		case Sigma:
			for(MetricType mt : mts) {
				XYSeries sigmaToTP = getSigmaSeries(wt, mt, forChart);
				xyseriescollection.addSeries(sigmaToTP);
			}
			break;
		default:
			System.out.println("We do not try to evaluate parameter "+ pt +" in test for Predra");
			return null;
		}
		return xyseriescollection;
	}
	
	public XYDataset getPredraAbsDataset(WorkLoadType wt, ParameterType pt, MetricType mt, boolean forChart) {
		XYSeriesCollection xyseriescollection = new XYSeriesCollection();
		switch(pt) {
		case Delta:
			XYSeries deltaS = getDeltaSeries(wt, mt, forChart);
			xyseriescollection.addSeries(deltaS);
			break;
		case Beta:
			XYSeries betaS = getBetaSeries(wt, mt, forChart);
			xyseriescollection.addSeries(betaS);
			break;
		case Gamma:
			XYSeries gammaS = getGammaSeries(wt, mt, forChart);
			xyseriescollection.addSeries(gammaS);
			break;
		case Sigma:
			XYSeries sigmaS = getSigmaSeries(wt, mt, forChart);
			xyseriescollection.addSeries(sigmaS);
			break;
		default:
			System.out.println("We do not try to evaluate parameter "+ pt +" in test for Predra");
			return null;
		}
		return xyseriescollection;
	}
	
	public XYSeriesCollection getPredraCompDataset(WorkLoadType wt, ParameterType pt, MetricType mt, boolean forChart) {
		XYSeriesCollection xyseriescollection = new XYSeriesCollection();
		switch(pt) {
		case Delta:
			XYSeries deltaS = getCompDeltaSeries(wt, mt, forChart);
			if(deltaS != null) xyseriescollection.addSeries(deltaS);
			break;
		case Beta:
			XYSeries betaS = getCompBetaSeries(wt, mt, forChart);
			if(betaS != null) xyseriescollection.addSeries(betaS);
			break;
		case Gamma:
			XYSeries gammaS = getCompGammaSeries(wt, mt, forChart);
			if(gammaS != null) xyseriescollection.addSeries(gammaS);
			break;
		case Sigma:
			XYSeries sigmaS = getCompSigmaSeries(wt, mt, forChart);
			if(sigmaS != null) xyseriescollection.addSeries(sigmaS);
			break;
		default:
			System.out.println("We do not try to evaluate parameter "+ pt +" in test for Predra");
			return null;
		}
		return xyseriescollection;
	}
	
	private DefaultCategoryDataset getMRReqSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		DefaultCategoryDataset paraToMt = new DefaultCategoryDataset();
		for(int i=0; i< ParaRanges.MRReqGRoups.length; ++i) {
			int mapReq = ParaRanges.mapReqs[i];
			int reduceReq = ParaRanges.reduceReqs[i];
			// hadoop
			String hadoopValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.HADOOP, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double hadoopVal = hadoopValue == null ? error : Double.parseDouble(hadoopValue);
			paraToMt.addValue(hadoopVal, SystemType.HADOOP, String.valueOf(ParaRanges.MRReqGRoups[i]));
			// predra
			String predraValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double predraVal = predraValue == null ? error : Double.parseDouble(predraValue);
			paraToMt.addValue(predraVal, SystemType.PREDRA, String.valueOf(ParaRanges.MRReqGRoups[i]));
			// mror
			String mrorValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.MROCHESTRATOR, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double mrorVal = mrorValue == null ? error : Double.parseDouble(mrorValue);
			paraToMt.addValue(mrorVal, SystemType.MROCHESTRATOR, String.valueOf(ParaRanges.MRReqGRoups[i]));
			// admp
			String admpValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.ADMP, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double admpVal = admpValue == null ? error : Double.parseDouble(admpValue);
			paraToMt.addValue(admpVal, SystemType.ADMP, String.valueOf(ParaRanges.MRReqGRoups[i]));
			
		}
		return paraToMt;
	}
	
	@Deprecated
	private DefaultCategoryDataset getMRReqCompSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		DefaultCategoryDataset paraToMt = new DefaultCategoryDataset();
		for(int i=0; i< ParaRanges.MRReqGRoups.length; ++i) {
			int mapReq = ParaRanges.mapReqs[i];
			int reduceReq = ParaRanges.reduceReqs[i];
			// hadoop
			String hadoopValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.HADOOP, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double hadoopVal = hadoopValue == null ? error : Double.parseDouble(hadoopValue);
			//paraToMt.addValue(hadoopVal, SystemType.HADOOP, String.valueOf(ParaRanges.MRReqGRoups[i]));
			// predra
			String predraValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double predraVal = predraValue == null ? error : Double.parseDouble(predraValue);
			double val = error;
			if(hadoopVal > 0 && predraVal > 0) {
				val = (predraVal - hadoopVal) / hadoopVal;
			}
			paraToMt.addValue(val, SystemType.PREDRA, String.valueOf(ParaRanges.MRReqGRoups[i]));
			// mror
			String mrorValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.MROCHESTRATOR, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double mrorVal = mrorValue == null ? error : Double.parseDouble(mrorValue);
			val = error;
			if(hadoopVal > 0 && mrorVal > 0) {
				val = (mrorVal - hadoopVal) / hadoopVal;
			}
			paraToMt.addValue(val, SystemType.MROCHESTRATOR, String.valueOf(ParaRanges.MRReqGRoups[i]));
			// admp
			String admpValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.ADMP, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double admpVal = admpValue == null ? error : Double.parseDouble(admpValue);
			val = error;
			if(hadoopVal > 0 && admpVal > 0) {
				val = (admpVal - hadoopVal) / hadoopVal;
			}
			paraToMt.addValue(val, SystemType.ADMP, String.valueOf(ParaRanges.MRReqGRoups[i]));
		}
		return paraToMt;
	}
	
	private DefaultCategoryDataset getMRReqCompSeries(WorkLoadType wt, MetricType mt, boolean forChart, boolean isTimeMetric) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		DefaultCategoryDataset paraToMt = new DefaultCategoryDataset();
		for(int i=0; i< ParaRanges.MRReqGRoups.length; ++i) {
			int mapReq = ParaRanges.mapReqs[i];
			int reduceReq = ParaRanges.reduceReqs[i];
			
			// predra
			String predraValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double predraVal = predraValue == null ? error : Double.parseDouble(predraValue);
			
			// hadoop
			String hadoopValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.HADOOP, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double hadoopVal = hadoopValue == null ? error : Double.parseDouble(hadoopValue);
			double val = error;
			if(hadoopVal > 0 && predraVal > 0) {
				if(isTimeMetric) {
					val = (hadoopVal - predraVal) / hadoopVal;
				}else {
					val = (predraVal - hadoopVal) / hadoopVal;
				}
			}
			paraToMt.addValue(val, SystemType.HADOOP, String.valueOf(ParaRanges.MRReqGRoups[i]));

			// mror
			String mrorValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.MROCHESTRATOR, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double mrorVal = mrorValue == null ? error : Double.parseDouble(mrorValue);
			val = error;
			if(mrorVal > 0 && predraVal > 0) {
				if(isTimeMetric) {
					val = (mrorVal - predraVal) / mrorVal;
				}else {
					val = (predraVal - mrorVal) / mrorVal;
				}
			}
			paraToMt.addValue(val, SystemType.MROCHESTRATOR, String.valueOf(ParaRanges.MRReqGRoups[i]));
			
			// admp
			String admpValue = getValue(wt, DefaultParas.reduceNumGroup, SystemType.ADMP, mapReq,
					reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double admpVal = admpValue == null ? error : Double.parseDouble(admpValue);
			val = error;
			if(admpVal > 0 && predraVal > 0) {
				if(isTimeMetric) {
					val = (admpVal - predraVal) / admpVal;
				}else {
					val = (predraVal - admpVal) / admpVal;
				}
			}
			paraToMt.addValue(val, SystemType.ADMP, String.valueOf(ParaRanges.MRReqGRoups[i]));
		}
		return paraToMt;
	}
	
	private DefaultCategoryDataset getReduceGroupNumSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		DefaultCategoryDataset paraToMt = new DefaultCategoryDataset();
		for(int redGroup : ParaRanges.reduceNumGroups) {
			// hadoop
			String hadoopValue = getValue(wt, redGroup, SystemType.HADOOP, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double hadoopVal = hadoopValue == null ? error : Double.parseDouble(hadoopValue);
			paraToMt.addValue(hadoopVal, SystemType.HADOOP, String.valueOf(redGroup));
			
			// predra
			String predraValue = getValue(wt, redGroup, SystemType.PREDRA, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double predraVal = predraValue == null ? error : Double.parseDouble(predraValue);
			paraToMt.addValue(predraVal, SystemType.PREDRA, String.valueOf(redGroup));
			
			// mror
			String mrorValue = getValue(wt, redGroup, SystemType.MROCHESTRATOR, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double mrorVal = mrorValue == null ? error : Double.parseDouble(mrorValue);
			paraToMt.addValue(mrorVal, SystemType.MROCHESTRATOR, String.valueOf(redGroup));
			
			// admp
			String admpValue = getValue(wt, redGroup, SystemType.ADMP, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double admpVal = admpValue == null ? error : Double.parseDouble(admpValue);
			paraToMt.addValue(admpVal, SystemType.ADMP, String.valueOf(redGroup));
			
		}
		return paraToMt;
	}
	
	@Deprecated
	private DefaultCategoryDataset getReduceGroupNumCompSeries(WorkLoadType wt, MetricType mt, boolean forChart) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		DefaultCategoryDataset paraToMt = new DefaultCategoryDataset();
		for(int redGroup : ParaRanges.reduceNumGroups) {
			// hadoop
			String hadoopValue = getValue(wt, redGroup, SystemType.HADOOP, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double hadoopVal = hadoopValue == null ? error : Double.parseDouble(hadoopValue);
			//paraToMt.addValue(hadoopVal, SystemType.HADOOP, String.valueOf(redGroup));
			// predra
			String predraValue = getValue(wt, redGroup, SystemType.PREDRA, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double predraVal = predraValue == null ? error : Double.parseDouble(predraValue);
			double val = error;
			if(hadoopVal > 0 && predraVal > 0) {
				val = (predraVal - hadoopVal) / hadoopVal;
			}
			paraToMt.addValue(val, SystemType.PREDRA, String.valueOf(redGroup));
			// mror
			String mrorValue = getValue(wt, redGroup, SystemType.MROCHESTRATOR, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double mrorVal = mrorValue == null ? error : Double.parseDouble(mrorValue);
			val = error;
			if(hadoopVal > 0 && mrorVal > 0) {
				val = (mrorVal - hadoopVal) / hadoopVal;
			}
			paraToMt.addValue(val, SystemType.MROCHESTRATOR, String.valueOf(redGroup));
			// admp
			String admpValue = getValue(wt, redGroup, SystemType.ADMP, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double admpVal = admpValue == null ? error : Double.parseDouble(admpValue);
			val = error;
			if(hadoopVal > 0 && admpVal > 0) {
				val = (admpVal - hadoopVal) / hadoopVal;
			}
			paraToMt.addValue(val, SystemType.ADMP, String.valueOf(redGroup));
		}
		return paraToMt;
	}
	
	private DefaultCategoryDataset getReduceGroupNumCompSeries(WorkLoadType wt, MetricType mt, boolean forChart, boolean isTimeMetric) {
		double error = forChart ? CHART_ERROR : DATA_ERROR;
		DefaultCategoryDataset paraToMt = new DefaultCategoryDataset();
		for(int redGroup : ParaRanges.reduceNumGroups) {
			
			// predra
			String predraValue = getValue(wt, redGroup, SystemType.PREDRA, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double predraVal = predraValue == null ? error : Double.parseDouble(predraValue);
			
			// hadoop
			String hadoopValue = getValue(wt, redGroup, SystemType.HADOOP, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double hadoopVal = hadoopValue == null ? error : Double.parseDouble(hadoopValue);
			double val = error;
			if(hadoopVal > 0 && predraVal > 0) {
				if(isTimeMetric) {
					val = (hadoopVal - predraVal) / hadoopVal;
				}else {
					val = (predraVal - hadoopVal) / hadoopVal;
				}
			}
			paraToMt.addValue(val, SystemType.HADOOP, String.valueOf(redGroup));

			// mror
			String mrorValue = getValue(wt, redGroup, SystemType.MROCHESTRATOR, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double mrorVal = mrorValue == null ? error : Double.parseDouble(mrorValue);
			val = error;
			if(mrorVal > 0 && predraVal > 0) {
				if(isTimeMetric) {
					val = (mrorVal - predraVal) / mrorVal;
				}else {
					val = (predraVal - mrorVal) / mrorVal;
				}
			}
			paraToMt.addValue(val, SystemType.MROCHESTRATOR, String.valueOf(redGroup));
			// admp
			String admpValue = getValue(wt, redGroup, SystemType.ADMP, DefaultParas.mapReq,
					DefaultParas.reduceReq, DefaultParas.delta, DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, 
					mt);
			double admpVal = admpValue == null ? error : Double.parseDouble(admpValue);
			val = error;
			if(admpVal > 0 && predraVal > 0) {
				if(isTimeMetric) {
					val = (admpVal - predraVal) / admpVal;
				}else {
					val = (predraVal - admpVal) / admpVal;
				}
			}
			paraToMt.addValue(val, SystemType.ADMP, String.valueOf(redGroup));
		}
		return paraToMt;
	}
	
	
	public DefaultCategoryDataset getMRRGDataset(WorkLoadType wt, ParameterType pt, MetricType mt, boolean forChart) {
		switch(pt) {
		case ReducesGroup:
			return getReduceGroupNumSeries(wt, mt, forChart);
		case MRReq:
			return getMRReqSeries(wt, mt, forChart);
		default:
			System.out.println("We do not try to evaluate parameter "+ pt +" in test for comparison");
			return null;
		}
	}
	
	public DefaultCategoryDataset getMRRGCompDataset(WorkLoadType wt, ParameterType pt, MetricType mt, boolean forChart) {
		switch(pt) {
		case ReducesGroup:
			return getReduceGroupNumCompSeries(wt, mt, forChart, MetricType.isTimeMetric(mt));
		case MRReq:
			return getMRReqCompSeries(wt, mt, forChart, MetricType.isTimeMetric(mt));
		default:
			System.out.println("We do not try to evaluate parameter "+ pt +" in test for comparison");
			return null;
		}
	}
	
	public XYDataset getCDFDataset(WorkLoadType wt, int reduceNumGroup, SystemType st, int mapReqMB,
			int reduceReqMB, float delta, float beta, float gamma, float sigma, int min, int max) {
		if(st != SystemType.PREDRA) return null;
		XYSeriesCollection xyseriescollection = new XYSeriesCollection();
		String key = wt.toString() + " " + reduceNumGroup + " " + st.toString() + " " + mapReqMB + " " + reduceReqMB 
				+ " " + delta + " " + beta + " " + gamma + " " + sigma;
		TreeMap<Integer, Float> cdf = CDFs.get(key);
		if(cdf != null) {
			XYSeries cdfSeries = new XYSeries("CDF");
			for(Map.Entry<Integer, Float> kv : cdf.entrySet()) {
				int keey = kv.getKey();
				if(keey >= min && keey <= max)
					cdfSeries.add(kv.getKey(), kv.getValue()); 
			}
			xyseriescollection.addSeries(cdfSeries);
			return xyseriescollection;
		}
		return null;
	}
	
	public XYDataset getCDFDataset(WorkLoadType wt, int minDiffMB, int maxDiffMB) {
		return getCDFDataset(wt, DefaultParas.reduceNumGroup, SystemType.PREDRA, DefaultParas.mapReq, DefaultParas.reduceReq, DefaultParas.delta,
				DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma, minDiffMB, maxDiffMB);
	}
	
	public XYDataset getMemUADataset(WorkLoadType wt, int reduceNumGroup, SystemType st, int mapReqMB,
			int reduceReqMB, float delta, float beta, float gamma, float sigma) {
		if(st != SystemType.HADOOP) return null;
		XYSeriesCollection xyseriescollection = new XYSeriesCollection();
		String key = wt.toString() + " " + reduceNumGroup + " " + st.toString() + " " + mapReqMB + " " + reduceReqMB 
				+ " " + delta + " " + beta + " " + gamma + " " + sigma;
		TreeMap<Long, MemUAPair> memUASeries = uaSeriesMap.get(key);
		if(memUASeries != null) {
			XYSeries tuSeries = new XYSeries("MemUsed");
			XYSeries taSeries = new XYSeries("MemAlloc");
			for(Map.Entry<Long, MemUAPair> kv : memUASeries.entrySet()) {
				long time = kv.getKey();
				long memUsed = kv.getValue().getMemUsed();
				long memAlloc = kv.getValue().getMemAlloc();
				if(time > 0 && time < Long.MAX_VALUE) {
					tuSeries.add(time, memUsed);
					taSeries.add(time, memAlloc);
				}
			}
			xyseriescollection.addSeries(tuSeries);
			xyseriescollection.addSeries(taSeries);
			return xyseriescollection;
		}
		return null;
	}
	
	public XYDataset getMemUADataset(WorkLoadType wt, int mapReq, int redReq) {
		return getMemUADataset(wt, DefaultParas.reduceNumGroup, SystemType.HADOOP, mapReq, redReq, DefaultParas.delta,
				DefaultParas.beta, DefaultParas.gamma, DefaultParas.sigma);
	}

	// for test
	//public static void main(String args[]) {
	//	ZCDatasets haha = new ZCDatasets("/home/zc/ZCBenchmarkSuite/analyseResult", colNameOfCDF);
	//	System.out.println(haha.getValue(WorkLoadType.SNS, 1, SystemType.PREDRA, 1024, 2048, 0.05f, 1.5f, 2f, 0.5f, MetricType.TP));
	//}
	
	
	////////////////////// for test ///////////////////////////
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
