package nlsde.buaa.groupmobility;

public class StreamChartBean {
	private int time;
	private int downCount;
	private int downRange;
	
	private int upCount;
	private int upRange;
	
	public StreamChartBean(int time){
		this.time=time;
		this.downCount=0;
		this.downRange=0;
		this.upCount=0;
		this.upRange=0;
	}
	
	public void add(){
		this.upCount++;
		this.upRange=this.getRange(this.upCount, this.upRange);
	}
	
	public int getRange(int c,int r){
		return 0;//TODO:range calculate
	}
	
	@Override
	public String toString(){
		return this.time+":00,"+this.downCount+","+this.downRange+","+this.upCount+","+this.upRange;
	}
}
