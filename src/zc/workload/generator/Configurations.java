package zc.workload.generator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Configurations {
	
	private static String path = null;
	private HashMap<String, String> conf_cache;
	private static Configurations instance = null;
	
	private Configurations(String confFile) throws IOException {
		path = confFile;
		conf_cache = new HashMap<String, String>();
		BufferedReader br;
		br = new BufferedReader(new FileReader(confFile));
		String oneLine = br.readLine();
		while(oneLine != null) {
			if(!oneLine.trim().startsWith("#") && !oneLine.trim().equals("")) {
				String kv[] = oneLine.trim().split("=");
				if(kv.length == 2)
					conf_cache.put(kv[0], kv[1]);
			}
			oneLine = br.readLine();
		}
		br.close();
	}
	
	public static Configurations getInstance(String confFile) {
		if(instance == null) {
			try {
				instance = new Configurations(confFile);
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		} else if(!confFile.equals(path)){
			try {
				instance = new Configurations(confFile);
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
		return instance;
	}
	
	public String get(String key) {
		return conf_cache.get(key);
	}
	
	public void put(String key, String value) {
		conf_cache.put(key, value);
	}
}
