package zc.workload.generator;


/**
 * For Social Network  group 5.
 * @author zc
 *
 */
public class WordCountScript2 extends WordCountCommon {

	private String BASE_IN_DIR_2 = "wordcount_input_basedir_2";
	private String BASE_OUT_DIR_2 = "wordcount_output_basedir_2";
	
	public WordCountScript2(String confFile, int gid, String scriptInfo, int jobNum, int reds, int reds2) {
		super(confFile, gid, scriptInfo, jobNum, reds, reds2);
		BASE_IN_DIR_2 = conf.get(BASE_IN_DIR_2);
		BASE_OUT_DIR_2 = conf.get(BASE_OUT_DIR_2);
	}
	
	@Override
	public String nextScript() {
		if(counter > 0) {
			counter --;
			String script = EXECUTABLE_DIR + "/run_2.sh" + " " + counter + " " + BASE_IN_DIR_2 + " " + reduces;
			return script;
		}
		return null;
	}

}
