package zc.workload.generator;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * For Search Engine group 1.
 * @author zc
 *
 */
public class WordCountScript3 extends WordCountCommon {
	
	private String BASE_IN_DIR_3 = "wordcount_input_basedir_3";
	private String BASE_OUT_DIR_3 = "wordcount_output_basedir_3";

	private ArrayList<String> hdfsInputFiles_3;

	public WordCountScript3(String confFile, int gid, String scriptInfo,
			int jobNum, int reds, int reds2) {
		super(confFile, gid, scriptInfo, jobNum, reds, reds2);
		BASE_IN_DIR_3 = conf.get(BASE_IN_DIR_3);
		BASE_OUT_DIR_3 = conf.get(BASE_OUT_DIR_3);
		hdfsInputFiles_3 = new ArrayList<>();
		try {
			loadInputFiles(hdfsInputFiles_3, BASE_IN_DIR_3);
		} catch (NullPointerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String nextScript() {
		if(counter > 0 && hdfsInputFiles_3.size() > 0) {
			String inputHdfsFile = hdfsInputFiles_3.get(counter % hdfsInputFiles_3.size());
			counter --;
			//String outHdfsFile = BASE_OUT_DIR + "_" + counter;
			String script = EXECUTABLE_DIR + "/run_3.sh" + " " + counter + " " + inputHdfsFile + " " + reduces;
			return script;
		}
		return null;
	}
}
