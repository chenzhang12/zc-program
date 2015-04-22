package zc.workload.generator;

/**
 * For Search Engine group 6.
 * @author zc
 *
 */
public class WordCountScript4 extends WordCountCommon {
	
	private String BASE_IN_DIR_4 = "wordcount_input_basedir_4";
	private String BASE_OUT_DIR_4 = "wordcount_output_basedir_4";
	
	public WordCountScript4(String confFile, int gid, String scriptInfo, int jobNum, int reds, int reds2) {
		super(confFile, gid, scriptInfo, jobNum, reds, reds2);
		BASE_IN_DIR_4 = conf.get(BASE_IN_DIR_4);
		BASE_OUT_DIR_4 = conf.get(BASE_OUT_DIR_4);
	}
	
	@Override
	public String nextScript() {
		if(counter > 0) {			
			counter --;
			String script = EXECUTABLE_DIR + "/run_4.sh" + " " + counter + " " + BASE_IN_DIR_4 + " " + reduces;
			return script;
		}
		return null;
	}

}
