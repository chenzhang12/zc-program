package zc.workload.generator;

import java.io.IOException;
import java.util.ArrayList;

public class GrepScript1 extends GrepCommon {
	
	private String BASE_IN_DIR_1 = "grep_input_basedir_1";
	private String BASE_OUT_DIR_1 = "grep_output_basedir_1";
	
	private ArrayList<String> hdfsInputFiles_1;

	public GrepScript1(String confFile, int gid, String scriptInfo, int jobNum, int grepReds, int sortReds) {
		super(confFile, gid, scriptInfo, jobNum, grepReds, sortReds); 
		BASE_IN_DIR_1 = conf.get(BASE_IN_DIR_1);
		BASE_OUT_DIR_1 = conf.get(BASE_OUT_DIR_1);	
		hdfsInputFiles_1 = new ArrayList<>();
		try {
			loadInputFiles(hdfsInputFiles_1, BASE_IN_DIR_1);
		} catch (NullPointerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String nextScript() {
		if(counter > 0 && hdfsInputFiles_1.size() > 0) {
			String inputHdfsFile = hdfsInputFiles_1.get(counter % hdfsInputFiles_1.size());
			counter --;
			//String outHdfsFile = BASE_OUT_DIR + "_" + counter;
			String script = EXECUTABLE_DIR + "/run_grep_1.sh" + " " + counter + " " + inputHdfsFile + " " + reduces + " " + reduces2;
			return script;
		}
		return null;
	}

}
