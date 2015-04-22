package zc.workload.generator;

public class SortCommon extends CommonScript {

	protected String EXECUTABLE_DIR = "sort_executable_basedir";
	
	public SortCommon(String confFile, int gid, String scriptInfo, int jobNum, int reds, int reds2) {
		super(confFile, gid, scriptInfo, jobNum, reds, reds2);
		EXECUTABLE_DIR = "$BASEDIR/" + conf.get(EXECUTABLE_DIR);
	}

}
