package yy.nlsde.buaa.region;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class RegionCountBean extends RegionBean{
	public List<PointBean> stations;
	
	HashMap<String,RegionChartBean> chartlist;

	private int count=0;
	
	public int getCount() {
		return count;
	}
	
	public void addCount(int count){
		this.count+=count;
	}
	
	public void addChartCount(String time,int ud, int count){
		if (chartlist==null){
			chartlist=new HashMap<String,RegionChartBean>();
		}
		if (!chartlist.containsKey(time)){
			chartlist.put(time, new RegionChartBean(time+":00"));
		}
		chartlist.get(time).add(ud,	count);
	}
	
	public String toString(){
		if (this.points==null)
			buildEdge();
		int nv=this.points.size();
		String re=this.count+","+nv;
		for (PointBean p:this.points){
			re+=","+p.toString();
		}
		return re;
	}
	
	public String getRegionEdgeString(){
		String re="";
		boolean first=true;
		for (PointBean p:this.points){
			if (first){
				first=false;
			}else{
				re+=",";
			}
			re+=p.toAreaString();
		}
		return re;
	}
	
	public void addStation(PointBean p){
		if (stations==null)
			stations=new ArrayList<PointBean>();
		stations.add(p);
	}
	
	public void buildEdge(){
		this.points=RegionUtil.Points2RegionEdge(this.stations);
	}
}
