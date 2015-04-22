package zc.workload.generator;

import java.io.IOException;
import java.util.ArrayList;

public class SortScript2 extends SortCommon {
	
	private String BASE_IN_DIR_2 = "sort_input_basedir_2";
	private String BASE_OUT_DIR_2 = "sort_output_basedir_2";

	private ArrayList<String> hdfsInputFiles_2;

	public SortScript2(String confFile, int gid, String scriptInfo, int jobNum, int reds, int reds2) {
		super(confFile, gid, scriptInfo, jobNum, reds, reds2);
		BASE_IN_DIR_2 = conf.get(BASE_IN_DIR_2);
		BASE_OUT_DIR_2 = conf.get(BASE_OUT_DIR_2);	
		hdfsInputFiles_2 = new ArrayList<>();
		try {
			loadInputFiles(hdfsInputFiles_2, BASE_IN_DIR_2);
		} catch (NullPointerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String nextScript() {
		if(counter > 0 && hdfsInputFiles_2.size() > 0) {
			String inputHdfsFile = hdfsInputFiles_2.get(counter % hdfsInputFiles_2.size());
			counter --;
			//String outHdfsFile = BASE_OUT_DIR + "_" + counter;
			String script = EXECUTABLE_DIR + "/run_2.sh" + " " + counter + " " + inputHdfsFile + " " + reduces;
			return script;
		}
		return null;
	}

}
