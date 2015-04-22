package zc.workload.generator;

public class IndexScript extends CommonScript {
	
	protected String EXECUTABLE_DIR = "index_executable_basedir";
	
	private String BASE_IN_DIR = "index_input_basedir_1";
	private String BASE_OUT_DIR = "index_output_basedir_1";

	public IndexScript(String confFile, int gid, String scriptInfo, int jobNum, int reds, int reds2) {
		super(confFile, gid, scriptInfo, jobNum, reds, reds2);
		EXECUTABLE_DIR = "$BASEDIR/" + conf.get(EXECUTABLE_DIR);
		BASE_IN_DIR = conf.get(BASE_IN_DIR);
		BASE_OUT_DIR = conf.get(BASE_OUT_DIR);
	}
	
	@Override
	public String nextScript() {
		if(counter > 0) {
			counter --;
			String script = EXECUTABLE_DIR + "/run.sh" + " " + counter + " " + reduces;
			return script;
		}
		return null;
	}

}
