package zc.workload.generator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class CommonScript implements ScriptWriter {
	
	protected int groupId;
	protected String scriptInfo;
	protected int counter;
	protected int reduces;
	protected int reduces2;

	protected String HADOOP_HOME = "HADOOP_HOME";
	protected String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
	@Deprecated
	protected String HIBENCH_HOME = "HIBENCH_HOME";
	@Deprecated
	protected String BIGDATABENCH_HOME = "BIGDATABENCH_HOME";
	
	
	protected Configurations conf;
	protected Configuration hConf;
	
	public CommonScript(String confFile, int gid, String scriptInfo, int jobNum, int reduceNum, int reduceNum2) {
		conf = Configurations.getInstance(confFile);
		HADOOP_HOME = conf.get(HADOOP_HOME);
		HADOOP_CONF_DIR = conf.get(HADOOP_CONF_DIR);
		HIBENCH_HOME = conf.get(HIBENCH_HOME);
		BIGDATABENCH_HOME = conf.get(BIGDATABENCH_HOME);
		groupId = gid;
		this.scriptInfo = scriptInfo;
		counter = jobNum;
		reduces = reduceNum;
		reduces2 = reduceNum2;
		hConf = new Configuration();
		loadHadoopConf(hConf);
	}
		
	private void loadHadoopConf(Configuration hconf) {
		try {
			if(HADOOP_CONF_DIR == null || HADOOP_CONF_DIR.trim().equals("")) {
				HADOOP_CONF_DIR = HADOOP_HOME + "/etc/hadoop";
			}
			FileInputStream fin1 = new FileInputStream(HADOOP_CONF_DIR + "/core-site.xml");
			FileInputStream fin2 = new FileInputStream(HADOOP_CONF_DIR + "/hdfs-site.xml");		
			hconf.addResource(fin1);
			hconf.addResource(fin2);
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	protected void loadInputFiles(List<String> cache, String inputDir) throws NullPointerException, IOException {
		if(cache == null || inputDir == null) 
			throw new NullPointerException("Please make sure the cache and input dir are not null !");
		FileSystem dfs = FileSystem.get(hConf);
		RemoteIterator<LocatedFileStatus> inputFiles = dfs.listFiles(new Path(inputDir), false);
		while(inputFiles.hasNext()) {
			LocatedFileStatus status = inputFiles.next();
			if(status.isFile()) {
				String fileName = status.getPath().toString();
				if(fileName != null && !fileName.trim().equals("") && !fileName.endsWith("_SUCCESS")) {
					cache.add(fileName);
				}
			}
		}
	}
	
	@Override
	public String nextScript() {
		// implement it in sub-classes !
		return null;
	}

	@Override
	public int getCounter() {
		return counter;
	}

}
