package zc.workload.generator;

public class GrepScript2 extends GrepCommon {

	private String BASE_IN_DIR_2 = "grep_input_basedir_2";
	private String BASE_OUT_DIR_2 = "grep_output_basedir_2";
	
	public GrepScript2(String confFile, int gid, String scriptInfo, int jobNum, int grepReds, int sortReds) {
		super(confFile, gid, scriptInfo, jobNum, grepReds, sortReds);
		BASE_IN_DIR_2 = conf.get(BASE_IN_DIR_2);
		BASE_OUT_DIR_2 = conf.get(BASE_OUT_DIR_2);
	}

	@Override
	public String nextScript() {
		if(counter > 0) {
			counter --;
			String script = EXECUTABLE_DIR + "/run_grep_2.sh" + " " + counter + " " + BASE_IN_DIR_2 + " " + reduces + " " + reduces2;
			return script;
		}
		return null;
	}
}
