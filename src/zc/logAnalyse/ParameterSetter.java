package zc.logAnalyse;

import java.io.Closeable;

public class ParameterSetter implements Closeable {
	
	private ConfigModifier yarnConf = null;
	private ConfigModifier mapRedConf = null;
	
	public ParameterSetter(String confDir) {
		String yarnFile = confDir + "/yarn-site.xml";
		yarnConf = new ConfigModifier(yarnFile, yarnFile);
		String mapRedFile = confDir + "/mapred-site.xml";
		mapRedConf = new ConfigModifier(mapRedFile, mapRedFile);
	}
	
	public void setMapMemReq(int mapMemReq) {
		mapRedConf.setInt("mapreduce.map.memory.mb", mapMemReq);
		int mapJavaHeap = (int)Math.ceil(mapMemReq * 0.8);
		String mapJavaOpt = "-Xmx" + mapJavaHeap + "m";
		mapRedConf.set("mapreduce.map.java.opts", mapJavaOpt);
	}
	
	public void setReduceMemReq(int reduceMemReq) {
		mapRedConf.setInt("mapreduce.reduce.memory.mb", reduceMemReq);
		int redJavaHeap = (int)Math.ceil(reduceMemReq * 0.8);
		String redJavaOpt = "-Xmx" + redJavaHeap + "m";
		mapRedConf.set("mapreduce.reduce.java.opts", redJavaOpt);
	}
	
	public void setDelta(float delta) {
		// yarn.nodemanager.estimator.increase.threshold-ratio.to-MI
		yarnConf.setFloat("yarn.nodemanager.estimator.increase.threshold-ratio.to-MI", delta);
	}
	
	public void setBeta(float beta) {
		// yarn.nodemanager.estimator.increase.increment-ratio.to-UM
		yarnConf.setFloat("yarn.nodemanager.estimator.increase.increment-ratio.to-UM", beta);
	}
	
	public void setGamma(float gamma) {
		// yarn.nodemanager.estimator.release.threshold-ratio.to-UM
		yarnConf.setFloat("yarn.nodemanager.estimator.release.threshold-ratio.to-UM", gamma);
	}
	
	public void setSigma(float sigma) {
		// yarn.nodemanager.estimator.release.slowdown
		yarnConf.setFloat("yarn.nodemanager.estimator.release.slowdown", sigma);
	}

	@Override
	public void close() {
		yarnConf.format();
		mapRedConf.format();
	}
	
	// test
	public static void main(String[] args) {
		
		if(args.length < 7) {
			System.out.println("usage: ParameterSetter <hadoopConfDir> <mapReq> <reduceReq> <delta> <beta> <gamma> <sigma>");
			return;
		}
		ParameterSetter ps = new ParameterSetter(args[0].trim());
		String mapReq = args[1].trim();
		String reduceReq = args[2].trim(); 
		String delta = args[3].trim();
		String beta = args[4].trim();
		String gamma = args[5].trim();
		String sigma = args[6].trim();
		
		ps.setMapMemReq(Integer.parseInt(mapReq));
		ps.setReduceMemReq(Integer.parseInt(reduceReq));
		ps.setDelta(Float.parseFloat(delta));
		ps.setBeta(Float.parseFloat(beta));
		ps.setGamma(Float.parseFloat(gamma));
		ps.setSigma(Float.parseFloat(sigma));
		ps.close();
	}
}
