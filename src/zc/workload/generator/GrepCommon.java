package zc.workload.generator;

public class GrepCommon extends CommonScript {
	
	protected String EXECUTABLE_DIR = "grep_executable_basedir";

	public GrepCommon(String confFile, int gid, String scriptInfo, int jobNum, int grepReds, int sortReds) {
		super(confFile, gid, scriptInfo, jobNum, grepReds, sortReds);
		EXECUTABLE_DIR = "$BASEDIR/" + conf.get(EXECUTABLE_DIR);
	}

}
