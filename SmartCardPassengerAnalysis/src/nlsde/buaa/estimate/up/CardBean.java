package nlsde.buaa.estimate.up;

public class CardBean {
	public final static int ENT = 1;// entry only
	public final static int DIS = 2;// distance
	
	private String line;//the data string
	private String lineId;
//	private String cardId;
	private String vehicleId;
	private String upSeq;
	private String downSeq;
	private String upTime;
	private String downTime;
	
	private boolean avilable = true;
	private int type;
	
	public CardBean(String line) {
		this.line = line;
		String info[] = line.split(",");
		if (info.length < 12) {
			avilable=false;
		} else {
//			cardId = info[6];
			lineId = info[9];
			vehicleId = info[10];
			upSeq = info[11];
			downSeq = info[12];
			if (Integer.parseInt(lineId)<600){
				// is entry only model data
				this.type=CardBean.ENT;
				upTime=info[2];
			}else{
				// is distance model data
				this.type=CardBean.DIS;
				downTime=info[2];
			}
			try{
				
			}catch(Exception e){
				e.printStackTrace();
				avilable=false;
			}
		}
	}
	
	public boolean isEntryOnly(){
		return this.type==CardBean.ENT;
	}
	
	public String getLine() {
		return line;
	}

	public void setLine(String line) {
		this.line = line;
	}

	public String getLineId() {
		return lineId;
	}

	public void setLineId(String lineId) {
		this.lineId = lineId;
	}

	public String getVehicleId() {
		return vehicleId;
	}

	public void setVehicleId(String vehicleId) {
		this.vehicleId = vehicleId;
	}

	public String getUpSeq() {
		return upSeq;
	}

	public void setUpSeq(String upSeq) {
		this.upSeq = upSeq;
	}

	public String getDownSeq() {
		return downSeq;
	}

	public void setDownSeq(String downSeq) {
		this.downSeq = downSeq;
	}

	public String getUpTime() {
		return upTime;
	}

	public void setUpTime(String upTime) {
		this.upTime = upTime;
	}

	public String getDownTime() {
		return downTime;
	}

	public void setDownTime(String downTime) {
		this.downTime = downTime;
	}

	public boolean isAvilable() {
		return avilable;
	}
}
