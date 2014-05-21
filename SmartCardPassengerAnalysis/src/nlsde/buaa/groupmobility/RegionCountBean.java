package nlsde.buaa.groupmobility;

import java.util.ArrayList;

public class RegionCountBean extends RegionBean{
	public final static int UP=1;
	public final static int DOWN=2;
	
	private int count;
	private int time;
	private int ud;
	
	public RegionCountBean(String line){
		String[] data=line.split(",");
		this.time=Integer.parseInt(data[0]);
		this.ud=Integer.parseInt(data[1]);
		this.count=Integer.parseInt(data[2]);
		int vn=Integer.parseInt(data[3]);
		this.points=new ArrayList<PointBean>();
		for (int i=0;i<vn;i++){
			String[] lonlat=data[i+4].split("-");
			this.points.add(new PointBean(Double.parseDouble(lonlat[0]),Double.parseDouble(lonlat[1])));
		}
		this.center=RegionUtil.getRegionCenter(this);
	}
	
	public int getCount(){
		return this.count;
	}
	
	public String getTime(){
		return ""+this.time;
	}
	
	public int getUD(){
		return this.ud;
	}
}
