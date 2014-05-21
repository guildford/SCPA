package nlsde.buaa.groupmobility;

public class PointBean {
	protected double lon;
	protected double lat;

	public PointBean() {
	}

	public PointBean(double lon, double lat) {
		this.lon = lon;
		this.lat = lat;
	}

	public double getLon() {
		return lon;
	}

	public double getLat() {
		return lat;
	}

	public String toString() {
		return lon + "," + lat;
	}
}
