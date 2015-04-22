package zc.workload.generator;

import java.io.IOException;
import java.util.ArrayList;

/**
 * For Social Network group 1.
 * @author zc
 *
 */
public class WordCountScript1 extends WordCountCommon {

	private String BASE_IN_DIR_1 = "wordcount_input_basedir_1";
	private String BASE_OUT_DIR_1 = "wordcount_output_basedir_1";

	private ArrayList<String> hdfsInputFiles_1;
	
	public WordCountScript1(String confFile, int groupId, String script, int jobNum, int reds, int reds2) {
		super(confFile, groupId, script, jobNum, reds, reds2);
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
			String script = EXECUTABLE_DIR + "/run_1.sh" + " " + counter + " " +inputHdfsFile + " " + reduces;
			return script;
		}
		return null;
	}

}
