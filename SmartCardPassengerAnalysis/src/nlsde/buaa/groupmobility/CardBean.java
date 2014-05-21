package nlsde.buaa.groupmobility;

import nlsde.buaa.base.util.CONSTANT;

public class CardBean {

	protected String line;
	protected String cardID;
	protected String lineID;

	protected String uplineID;
	protected String upSeq;
	protected String upLon;
	protected String upLat;
	protected String upTime;

	protected String downlineID;
	protected String downSeq;
	protected String downLon;
	protected String downLat;
	protected String downTime;

	protected boolean avilable = true;

	public boolean isAvilable() {
		return this.avilable;
	}

	public String getCardID() {
		return this.cardID;
	}

	public String getUpLon() {
		return this.upLon;
	}

	public String getUpLat() {
		return this.upLat;
	}

	public String getDownLon() {
		return this.downLon;
	}

	public String getDownLat() {
		return this.downLat;
	}

	public final static int HOUR = 1;
	public final static int MIN = 2;

	/**
	 * constructed from line witch in "CSV" file;
	 * 
	 * @param line
	 */
	public CardBean(String line, int type) {
		switch (type) {
		case CONSTANT.AFC:
			initialAFC(line);
			break;
		case CONSTANT.BUS:
			initialBUS(line);
			break;
		default:
			break;
		}
	}
	
	private void initialBUS(String line){
		this.line = line;
		String info[] = line.split(",");
		if (info.length<8){
			this.avilable=false;
			lineID="-1";
		}else{
			this.upLon=info[0];
			this.upLat=info[1];
			this.upTime=info[2];
			this.downLon=info[3];
			this.downLat=info[4];
			this.downTime=info[5];
			this.cardID=info[6];
			this.lineID=info[7];
		}
	}

	private void initialAFC(String line) {
		this.line = line;
		String info[] = line.split(",");
		if (info.length < 14) {
			this.avilable = false;
			lineID = "-1";
		} else {
			cardID = info[6];
			uplineID = info[11];
			upSeq = info[12];
			upTime = info[13];

			downlineID = info[14];
			downSeq = info[15];
			downTime = info[3];
		}
		String position = CONSTANT.get_STATION_TO_POSITION().get(
				this.uplineID + "," + this.upSeq);
		if (position == null || !position.contains(",")) {
			this.avilable = false;
			return;
		}
		String[] p = position.split(",");
		this.upLat = p[1];
		this.upLon = p[0];

		position = CONSTANT.get_STATION_TO_POSITION().get(
				this.downlineID + "," + this.downSeq);
		if (position == null || !position.contains(",")) {
			this.avilable = false;
			return;
		}
		p = position.split(",");
		this.downLat = p[1];
		this.downLon = p[0];
	}

	public boolean isAFCUp() {
		return "0".equals(this.downlineID);
	}

	public String getUpTime(int type) {
		int length = 6;
		switch (type) {
		case HOUR:
			length = 2;
			break;
		case MIN:
			length = 4;
			break;
		default:
			break;
		}
		if (this.upTime != null && this.upTime.length() > 8) {
			return this.upTime.substring(8, 8 + length);
		} else {
			return null;
		}
	}
	public String getDownTime(int type) {
		int length = 6;
		switch (type) {
		case HOUR:
			length = 2;
			break;
		case MIN:
			length = 4;
			break;
		default:
			break;
		}
		if (this.downTime != null && this.downTime.length() > 8) {
			return this.downTime.substring(8, 8 + length);
		} else {
			return null;
		}
	}
}
