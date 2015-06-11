package zc.logAnalyse;

public class UAScTuple {
	public long time;
	public long u; //MB
	public long a; //MB
	public int sc;
	public UAScTuple(long u, long a, int sc) {
		this.u = u;
		this.a = a;
		this.sc = sc;
	}
	public UAScTuple(long time, long u, long a, int sc) {
		this(u,a,sc);
		this.time = time;
	}
	public UAScTuple add(UAScTuple tuple) {
		u += tuple.u;
		a += tuple.a;
		sc += tuple.sc;
		return this;
	}
	public void add(long u, long a, int sc) {
		this.u += u;
		this.a += a;
		this.sc += sc;
	}
}
