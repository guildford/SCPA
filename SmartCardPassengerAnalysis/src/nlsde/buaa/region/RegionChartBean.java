package nlsde.buaa.region;

public class RegionChartBean{

	private String time;
	private int downCount;
	private int downRange;
	
	private int upCount;
	private int upRange;
	
	public RegionChartBean(String time){
		this.time=time;
		this.downCount=0;
		this.downRange=0;
		this.upCount=0;
		this.upRange=0;
	}
	
	public void add(int ud,int count){
		switch(ud){
		case PointCountBean.UP:
			this.upCount+=count;
			this.upRange=getRange(this.upCount,this.upRange);
			break;
		case PointCountBean.DOWN:
			this.downCount+=count;
			this.downRange=getRange(this.downCount,this.downRange);
		}
	}
	
	public int getRange(int c,int r){
		return 0;//TODO:range calculate
	}
	
	@Override
	public String toString(){
		return this.time+","+this.downCount+","+this.downRange+","+this.upCount+","+this.upRange;
	}
	
}
