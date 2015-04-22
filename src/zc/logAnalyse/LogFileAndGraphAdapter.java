package zc.logAnalyse;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LogFileAndGraphAdapter {

	String LOG_OUTPUT_DIR = "/home/zc/Desktop/smallTools/logAnalysisTools/hadoopLogs/analyse_result";
	String tp = "attempt_\\d+_\\d+_[m,r]_\\d+_\\d+";
	String tpm = "attempt_\\d+_\\d+_m_\\d+_\\d+";
	String tpr = "attempt_\\d+_\\d+_r_\\d+_\\d+";
	String appattemptId = null;
	String appattemptDir = null;

	int curIdx = -1;

	List<String> taids = null;

	public LogFileAndGraphAdapter(String appAttemptId, String taskType) {
		this(null, appAttemptId, taskType);
	}

	public LogFileAndGraphAdapter(String logOutputDir, String appAttemptId,  String taskType) {
		if (logOutputDir != null)
			this.LOG_OUTPUT_DIR = logOutputDir;
		this.appattemptId = appAttemptId;
		this.appattemptDir = LOG_OUTPUT_DIR + "/" + appattemptId;
		File f = new File(appattemptDir);
		if (!f.exists()) {
			System.err.println("No Application output dir found!");
			System.exit(1);
		}
		File[] files = f.listFiles();
		if (files == null || files.length == 0) {
			System.err.println("No output file found in app dir !");
			return;
		}
		if(taskType != null && taskType.equals("map")) {
			tp = tpm;
		} else if (taskType != null && taskType.equals("reduce")) {
			tp = tpr;
		}
		this.taids = new ArrayList<String>();
		for (File file : files) {
			String abspath = file.getAbsolutePath();
			String fileName = file.getName();
			if (fileName.matches(tp)) {
				taids.add(abspath);
			}
		}
		if (taids.size() == 0) {
			System.err.println("No task output file found in app dir!");
			return;
		}
	}

	public boolean hasLogFile() {
		if(taids.size() == 0) return false;
		return true;
	}
	
	public DataInputStream nextTaskFileInputStream() {
		curIdx++;
		if (curIdx < taids.size()) {
			String fileName = taids.get(curIdx);
			try {
				DataInputStream din = new DataInputStream(new FileInputStream(fileName));
				return din;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}			
		}
		return null;
	}

	public void reset() {
		curIdx = -1;
	}

	public static void main(String[] str) throws IOException {
		LogFileAndGraphAdapter l = new LogFileAndGraphAdapter(
				"appattempt_1390704303844_0001_000001", "reduce");
		DataInputStream din = null;
		while ((din = l.nextTaskFileInputStream()) != null) {
			String taskAttemptId = din.readUTF(); // task attempt id
			System.out.println(taskAttemptId);
			int len = din.readInt(); // record number
			System.out.println(len);
			String unit = din.readUTF();
			System.out.println(unit);
			for (int i = 0; i < len; ++i) {
				System.out.println(din.readLong());
				System.out.println(din.readDouble());
				System.out.println(din.readDouble());
			}
			din.close();
		}
	}
}
