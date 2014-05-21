package nlsde.buaa.estimate.up;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class UpEstimate {
	public static void main(String[] args){
		UpEstimate es=new UpEstimate();
		es.estimate("20130905");
	}

	private HashMap<String, List<String>> staMap;// <line_vehicle,list<station_time>>
	private HashMap<String, List<String>> timeMap;// <line_vehicle_staion,list<time>

	public void estimate(String date) {
		this.lineScheduleReadIn(date);
		CardBean card = null;
		// TODO:card read in
		this.estimate(card);
		// TODO:save card
	}

	private void lineScheduleReadIn(String date) {
		// read in the line schedule
		staMap=new HashMap<String,List<String>>();
		timeMap=new HashMap<String,List<String>>();
		String line="";//line_vehicle_station_time
		
		while(true){//TODO:read in each line
			String[] data=line.split("_");
			String stakey=data[0]+"_"+data[1];
			String stavalue=data[2]+"_"+data[3];
			if (!staMap.containsKey(stakey)){
				staMap.put(stakey, new ArrayList<String>());
			}
			staMap.get(stakey).add(stavalue);
			
			String timekey=data[0]+"_"+data[1]+"_"+data[2];
			String timevalue=data[3];
			if (!timeMap.containsKey(timekey)){
				timeMap.put(timekey, new ArrayList<String>());
			}
			timeMap.get(timekey).add(timevalue);
		}
	}

	private void estimate(CardBean card) {
		if (card.isEntryOnly()) {
			// entry only model
			String line = card.getLineId();
			String time = card.getUpTime();
			String vehicle = card.getVehicleId();
			String sta = getUpStation(line, vehicle, time);
			card.setUpSeq(sta);
		} else {
			// distance model
			String line = card.getLineId();
			String sta = card.getUpSeq();
			String vehicle = card.getVehicleId();
			String dtime = card.getDownTime();
			String time = getUpTime(line, vehicle, sta, dtime);
			card.setUpTime(time);
		}
	}

	private String getUpStation(String line, String vehicle, String time) {
		// get the up station from the schedule
		String sta = "-1";
		String key = line + "_" + vehicle;
		if (staMap.containsKey(key)) {
			List<String> list = staMap.get(key);
			int oldrange = -1;
			for (String sta_time : list) {
				String[] data = sta_time.split("_");
				//sta_time=station_time
				String ststa = data[0];
				String sttime = data[1];
				int range = Math.abs(Integer.parseInt(time)
						- Integer.parseInt(sttime));
				if (oldrange < 0) {
					oldrange = range;
				}
				if (range < ScheduleEstimate.STOP_MAX && range <= oldrange) {
					sta = ststa;
				}
			}
		}
		return sta;
	}

	private String getUpTime(String line, String vehicle, String seq,
			String dtime) {
		String utime="-1";
		String key=line+"_"+vehicle+"_"+seq;
		if (timeMap.containsKey(key)){
			List<String> list=timeMap.get(key);
			int oldrange = -1;
			for (String time:list){
				int range=Integer.parseInt(dtime)-Integer.parseInt(time);
				if (oldrange < 0) {
					oldrange = range;
				}
				if(range>0&&range<=oldrange){
					utime=time;
				}
			}
		}
		return utime;
	}
}
