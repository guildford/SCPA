package nlsde.buaa.groupmobility;

import java.util.HashMap;

public class StreamBean {

	private PointBean o;
	private PointBean d;
	private int count;
	private int time;
	
	public HashMap<Integer,StreamChartBean> chartlist; 
	
	public StreamBean(int time,PointBean o,PointBean d){
		this.time=time;
		this.o=o;
		this.d=d;
		this.count=0;
	}
	
	public String toString(){
		return this.time+","+this.o+","+this.d+","+this.count;
	}
	
	public String toStreamString(){
		return this.o+","+this.d+","+this.count;
	}
	
	public void addOne(int ut){
		if (this.time==ut)
			this.count++;
		else{
			if (chartlist==null){
				chartlist=new HashMap<Integer,StreamChartBean>();
			}
			if (!chartlist.containsKey(ut)){
				chartlist.put(ut, new StreamChartBean(ut));
			}
			chartlist.get(ut).add();
		}
	}
	
	public int getCount(){
		return this.count;
	}
	
}
