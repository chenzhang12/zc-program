package zc.workload.generator;

public class JobInstanceExhaustException extends Exception {
	private static final long serialVersionUID = 1L;
	public JobInstanceExhaustException(String msg) {
		super(msg);
	}
}
