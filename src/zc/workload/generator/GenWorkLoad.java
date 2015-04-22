package zc.workload.generator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
//import java.util.Random;

public class GenWorkLoad {
	
	private static int totalJobNumber = 0;
  //private static ArrayList<Script> submitScripts = new ArrayList<>();
  private static ArrayList<ScriptWriter> submitScripts = new ArrayList<>();
  private static String myConf = null;
  private static String sourceFileName;
  private static String destFileName;
  private static boolean useHistory = false;
  private static String historyFileName = null;
  private static DataInputStream historyReader = null;
  private static DataOutputStream historyWriter = null;
  private static int gidHead = 1;
  private static int timeHead = 2;
  private static int endMark = -100;
  private static Queue<Integer> gids = null;
  private static Queue<Integer> sleepTimes = null;
  
  static class OptionParser {
  	
  	private Map<String, String> option2Value = new HashMap<> ();
  	public OptionParser(String options[]) {
  		for(int i=0; i<options.length;) {
  			if(options[i].startsWith("--")) {
  				int j=i+1;
  				for(; j<options.length && !options[j].startsWith("--"); ++j) {
  					option2Value.put(options[i], options[j]);
  				}
  				i = j;
  			} else {
  				i ++;
  			}
  		}
  	}
  	
  	public String getOptionValue(String option) {
  		return option2Value.get(option);
  	}
  	
  	public boolean checkOptions(String ...options) {
  		for(String opToChk : options) {
	  		if(!option2Value.keySet().contains(opToChk)) return false;
  		}
  		return true;
  	}
  }

	public static void main(String args[]) {
		
		if(args.length < 8) {
			println("Please correctly input arguments. (e.g. GenWorkLoad --confFile /home/zc/Desktop/smallTools/gen_workloads/configure.sh --source /home/zc/Desktop/smallTools/gen_workloads/workloadSource --dest /home/zc/Desktop/smallTools/gen_workloads/runWorkload.sh --lambda 10 --genRandInterval 500 --useHistory true|false --history historyFile)");
    	return;
		}
		OptionParser op = new OptionParser(args);
		if(!op.checkOptions("--confFile","--source", "--dest", "--lambda", "--genRandInterval", "--useHistory", "--history")) {
			println("Please correctly input arguments. (e.g. GenWorkLoad --confFile /home/zc/Desktop/smallTools/gen_workloads/configure.sh --source /home/zc/Desktop/smallTools/gen_workloads/workloadSource --dest /home/zc/Desktop/smallTools/gen_workloads/runWorkload.sh --lambda 10 --genRandInterval 500 --useHistory true|false --history historyFile)");
			return;
		}
		
		myConf = op.getOptionValue("--confFile");
		int lambda = Integer.parseInt(op.getOptionValue("--lambda"));
		println("lambda is " + lambda);
		sourceFileName = op.getOptionValue("--source");
		println("source file is " + sourceFileName);
		destFileName = op.getOptionValue("--dest");
		println("dest file is " + destFileName);
		String uHis = op.getOptionValue("--useHistory");
		if(uHis != null && uHis.equals("true")) useHistory = true;
		historyFileName = op.getOptionValue("--history");
		if(historyFileName == null) {
			println("Please input history file name (e.g. GenWorkLoad --confFile /home/zc/Desktop/smallTools/gen_workloads/configure.sh --source /home/zc/Desktop/smallTools/gen_workloads/workloadSource --dest /home/zc/Desktop/smallTools/gen_workloads/runWorkload.sh --lambda 10 --genRandInterval 500 --useHistory true|false --history historyFile)");
			return;
		}
		println("History file is " + historyFileName);
		int genRandInterval = Integer.parseInt(op.getOptionValue("--genRandInterval"));
    generateScript(sourceFileName, destFileName, lambda, genRandInterval);
	}
	
	private static void init(String workloadSource) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(workloadSource));
		String oneLine = br.readLine();
		while(oneLine != null) {
			if(!oneLine.trim().startsWith("#") && !oneLine.trim().equals("")) {
				String idAndScript[] = oneLine.split(" ");
				int groupId = Integer.parseInt(idAndScript[0].trim());
				String scriptWriter = idAndScript[1].trim();
				int count = Integer.parseInt(idAndScript[2].trim());
				int reduceNum = -1;
				if(idAndScript.length > 3) {
					reduceNum = Integer.parseInt(idAndScript[3].trim());
				}
				int reduceNum2 = -1;
				if(idAndScript.length > 4) {
					reduceNum2 = Integer.parseInt(idAndScript[4].trim());
				}
				try {
					Class<ScriptWriter> swclazz = (Class<ScriptWriter>) Class.forName(scriptWriter);
					Constructor<ScriptWriter> csw = swclazz.getConstructor(new Class[]{String.class, int.class, String.class, int.class, int.class, int.class});
					ScriptWriter sw = csw.newInstance(myConf, groupId, scriptWriter, count, reduceNum, reduceNum2);
					totalJobNumber += count;
					submitScripts.add(sw);
				} catch (Exception e) {
					e.printStackTrace();
					br.close();
					return;
				}
			}
			oneLine = br.readLine();
		}
		br.close();
		initHistory();
	}
	
	public static void initHistory() throws IOException {
		if(useHistory) {
			historyReader = new DataInputStream(new FileInputStream(historyFileName));
			gids = new LinkedList<>();
			sleepTimes = new LinkedList<>();
			int mark = historyReader.readInt();
			while(mark != endMark) {
				if(mark == gidHead) {
					gids.add(historyReader.readInt());
				} else if(mark == timeHead) {
					sleepTimes.add(historyReader.readInt());
				}
				mark = historyReader.readInt();
			}
			historyReader.close();
		} else {
			historyWriter = new DataOutputStream(new FileOutputStream(historyFileName));
		}
	}

  public static void generateScript(String sourceScriptFile, String destScriptFile, double lambda, int genRandInterval) {
  	BufferedWriter bw = null;
  	try {		
			init(sourceScriptFile);
			bw = new BufferedWriter(new FileWriter(destScriptFile));
	  	double avg = 0;
			int i = 0;
			bw.write("#!/bin/bash \n\n");
			bw.write("# total group instance(job) number is " + totalJobNumber + "\n\n");		
			bw.write("THIS=`dirname \"$0\"`\nTHIS=`cd \"$THIS\"; pwd`\nTHIS=`readlink -f $THIS`\nBASEDIR=$THIS\n\n");
			for(i=0; i<totalJobNumber; ++i) {
			  int sleepSeconds = getPoisonNum(lambda);
				avg += sleepSeconds;
				try { if(!useHistory) Thread.sleep((long)(Math.random() * genRandInterval)); } catch (Exception e) {} // sleep for a while for better random seed.
				String sc = drawNextScript();
				if(sc != null) {
					bw.write(sc + " &\n");
					bw.write("sleep " + sleepSeconds + "\n\n");
				}
			} ///:~ for
			bw.write("# average arrival time in workload is " + avg/i + "\n");		
		} catch (IOException | JobInstanceExhaustException e1) {
			e1.printStackTrace();
		} finally {
			try {
				if(bw != null) bw.close();
				if(historyWriter != null) closeHistoryWriter();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
  }
  
  static int generatedJobInstances = 0;
  public static String drawNextScript() throws JobInstanceExhaustException {
  	int groupId = getRandGroupId();
  	if (groupId == -1) {
  		throw new JobInstanceExhaustException(
  				"The job instances in workload source ["+ sourceFileName + "] is not fully generated. Total [" + totalJobNumber + "] but generated [" + generatedJobInstances + "].");
  	}
  	ScriptWriter sc = submitScripts.get(groupId);
  	while(sc != null) {
  		String script = sc.nextScript();
  		if(script != null) {
  			generatedJobInstances ++;
  			return script;
  		}
			submitScripts.remove(groupId);
			groupId = getRandGroupId();
	  	if (groupId == -1) {
	  		throw new JobInstanceExhaustException(
	  				"The job instances in workload source ["+ sourceFileName + "] is not fully generated. Total [" + totalJobNumber + "] but generated [" + generatedJobInstances + "].");
	  	}
			sc = submitScripts.get(groupId);
  	}
  	return null;
  }
  
  public static int getRandGroupId() {
  	if(submitScripts.size() == 0) {
  		return recordId(-1);
  	}
  	if (useHistory) {
  		return gids.poll();
  	} else {
  		int id = (int) (Math.random() * submitScripts.size());
  		return recordId(id);
  	}
  }
  
  private static int recordId(int id) {
  	if(!useHistory) {
			try {
				historyWriter.writeInt(gidHead);
				historyWriter.writeInt(id);
			} catch (IOException e) {
				e.printStackTrace();
			}
  	}
  	return id;
  }
  
  public static int recordSleepTime(int st) {
  	if(!useHistory) {
			try {
				historyWriter.writeInt(timeHead);
				historyWriter.writeInt(st);
			} catch (IOException e) {
				e.printStackTrace();
			}
  	}
  	return st;
  }
  
  private static void closeHistoryWriter() throws IOException {
		historyWriter.writeInt(endMark);
		historyWriter.close();
  }
  
	public static int getPoisonNum(double Lamda){
		if(useHistory) {
			return sleepTimes.poll();
		} else {
			int x=0;
			double b=1,c=Math.exp(-Lamda),u;
			do {
				//Random r = new Random(System.currentTimeMillis());
				u = Math.random(); //r.nextDouble();
				b *= u;
				if(b>=c) x++;
			}while(b>=c);
			return recordSleepTime(x);
		}	
	}
	
	public static double println(double num) {
		System.out.println(num);
		return num;
	}
	
	public static String println(String line) {
		System.out.println(line);
		return line;
	}
}
