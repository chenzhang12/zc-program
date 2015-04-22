package zc.workload.generator;

public class KMeansScript extends CommonScript {
	
	protected String EXECUTABLE_DIR = "kmeans_executable_basedir";
	
	private String BASE_IN_DIR = "kmeans_input_basedir_1";
	private String BASE_OUT_DIR = "kmeans_output_basedir_1";

	public KMeansScript(String confFile, int gid, String scriptInfo, int jobNum, int reds, int reds2) {
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
