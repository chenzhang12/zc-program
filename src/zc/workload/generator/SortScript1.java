package zc.workload.generator;

public class SortScript1 extends SortCommon {
	
	private String BASE_IN_DIR_1 = "sort_input_basedir_1";
	private String BASE_OUT_DIR_1 = "sort_output_basedir_1";

	public SortScript1(String confFile, int gid, String scriptInfo, int jobNum, int reds, int reds2) {
		super(confFile, gid, scriptInfo, jobNum, reds, reds2);
		BASE_IN_DIR_1 = conf.get(BASE_IN_DIR_1);
		BASE_OUT_DIR_1 = conf.get(BASE_OUT_DIR_1);
	}
	
	@Override
	public String nextScript() {
		if(counter > 0) {
			counter --;
			String script = EXECUTABLE_DIR + "/run_1.sh" + " " + counter + " " + BASE_IN_DIR_1 + " " + reduces;
			return script;
		}
		return null;
	}

}
