package nlsde.buaa.groupmobility;

import java.util.ArrayList;
import java.util.List;

public class RegionBean {
	
	protected List<PointBean> points;
	
	protected PointBean center;
	
	public void addPoint(double lon, double lat) {
		if (points==null)
			points=new ArrayList<PointBean>();
		points.add(new PointBean(lon,lat));
	}
}
