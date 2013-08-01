package yy.nlsde.buaa.stationpassenger;

public class PointCountBean {
	public final static int UP=1;
	public final static int DOWN=2;

	private double lon;
	private double lat;
	private String time;
	private int ud;//up or down
	private int count;
	
	public PointCountBean(CardBean card,int ud){
		switch(ud){
		case UP:
			this.lon=Double.parseDouble(card.getUpLon());
			this.lat=Double.parseDouble(card.getUpLat());
			this.time=card.getUpTime(CardBean.HOUR);
			this.ud=UP;
			break;
		case DOWN:
			this.lon=Double.parseDouble(card.getDownLon());
			this.lat=Double.parseDouble(card.getDownLat());
			this.time=card.getDownTime(CardBean.HOUR);
			this.ud=DOWN;
			break;
		}
		this.count=0;
	}
	
	@Override
	public String toString(){
		return this.lon+","+this.lat+","+this.time+","+this.ud+","+this.count;
	}
	
	public String getKey(){
		return this.lon+","+this.lat+","+this.time+","+this.ud;
	}
	
	@Override
	public boolean equals(Object pcb){
		if (pcb instanceof PointCountBean){
			return this.getKey().equals(((PointCountBean)pcb).getKey());
		}else{
			return false;
		}
	}
	
	public void addOne(){
		this.count++;
	}
}
