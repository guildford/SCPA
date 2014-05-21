package nlsde.buaa.estimate.down;

public class PatternBean {
	public final static String SPLIT_CHAR = ",";
	
	protected static final int TIME = 0;
	protected static final int LON = 1;
	protected static final int LAT = 2;
	protected static final int W = 3;
	protected static final int[] PATTERN = { TIME, LON, LAT, W };

	protected double time;// 转化为分
	protected double lon;
	protected double lat;
	protected int weight;

	protected boolean avilable = true;
	

	public PatternBean(double time, double lon, double lat, int weight) {
		this.time = time;
		this.lon = lon;
		this.lat = lat;
		this.weight = weight;
	}

	protected PatternBean(CardBean card) {
		try {
			//TODO:临时改成小时
			String timestr = card.getUpTime(CardBean.HOUR);
			if (timestr == null || timestr.length() != 2) {
				this.avilable = false;
			}
//			this.time = Double.valueOf(timestr.substring(0, 2)) * 60;
//					+ Double.valueOf(timestr.substring(2, 4));
			this.time=Double.valueOf(timestr);
			this.lon = Double.valueOf(card.getUpLon());
			this.lat = Double.valueOf(card.getUpLat());
			this.weight = 1;
		} catch (Exception e) {
			this.avilable = false;
		}
	}

	protected PatternBean(String pbstr) {
		try {
			if (pbstr == null || pbstr.length() <= 0) {
				this.avilable = false;
			}
			String[] pbl = pbstr.split(PatternBean.SPLIT_CHAR);
			if (pbl.length != PatternBean.PATTERN.length ) {
				this.avilable = false;
			}
			this.time=Double.valueOf(pbl[PatternBean.TIME]);
			this.lon=Double.valueOf(pbl[PatternBean.LON]);
			this.lat=Double.valueOf(pbl[PatternBean.LAT]);
			this.weight=Integer.valueOf(pbl[PatternBean.W]);
		} catch (Exception e) {
			this.avilable = false;
		}
	}

	public boolean isAvilable() {
		return this.avilable;
	}

	public String toString() {
		return this.time+PatternBean.SPLIT_CHAR+
				this.lon+PatternBean.SPLIT_CHAR+
				this.lat+PatternBean.SPLIT_CHAR+
				this.weight;
	}

	public static PatternBean getPattern(CardBean card) {
		return new PatternBean(card);
	}

	public static PatternBean getPattern(String pbstr) {
		return new PatternBean(pbstr);
	}

	public double getTime() {
		return time;
	}

	public void setTime(double time) {
		this.time = time;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}
}
