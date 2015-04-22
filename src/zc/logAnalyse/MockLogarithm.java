package zc.logAnalyse;

public class MockLogarithm {
	
	public static final long FAIL = -1l;
	// set correction value to getValue(...) to complement the error.
	private static float CORRECTION_RATIO_TO_CURMEMUSED;
	
	// m(t) = log_r(t + 1) + c
	// x = ln(t + 1)
	// y = m(t)
	// b = 1/ln_r
	// a = c
	private double a;
	private double b;
	
	
	public Long getValue(Long x) {
		double xd = x.doubleValue();
		long ret = (long)Math.ceil(b * Math.log1p(xd) + a);
		if(ret <= 0l) return FAIL;
		return ret;
	}
	
	public Long getValue(Long x, Long ref) {
		Long est = getValue(x);
		if(est.longValue() == FAIL) return est;
		if(est.longValue() < ref.longValue() + getCorrection(ref)) {
			return ref.longValue() + getCorrection(ref);
		}
		return est;
	}
	
	private long getCorrection(long ref) {
		return (long)(CORRECTION_RATIO_TO_CURMEMUSED * ref);
	}
	
	

	public double getA() {
		return a;
	}

	public void setA(double a) {
		this.a = a;
	}

	public double getB() {
		return b;
	}

	public void setB(double b) {
		this.b = b;
	}

	public static void main(String[] args) {

	}

}
