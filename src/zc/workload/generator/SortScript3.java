package zc.workload.generator;

public class SortScript3 extends SortCommon {
	
	private String BASE_IN_DIR_3 = "sort_input_basedir_3";
	private String BASE_OUT_DIR_3 = "sort_output_basedir_3";

	public SortScript3(String confFile, int gid, String scriptInfo, int jobNum, int reds, int reds2) {
		super(confFile, gid, scriptInfo, jobNum, reds, reds2);
		BASE_IN_DIR_3 = conf.get(BASE_IN_DIR_3);
		BASE_OUT_DIR_3 = conf.get(BASE_OUT_DIR_3);
	}
	
	@Override
	public String nextScript() {
		if(counter > 0) {
			counter --;
			String script = EXECUTABLE_DIR + "/run_3.sh" + " " + counter + " " + BASE_IN_DIR_3 + " " + reduces;
			return script;
		}
		return null;
	}

}
