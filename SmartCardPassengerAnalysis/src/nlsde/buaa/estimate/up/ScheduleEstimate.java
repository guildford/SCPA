package nlsde.buaa.estimate.up;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class ScheduleEstimate {
	public static void main(String[] args){
		ScheduleEstimate est=new ScheduleEstimate();
		est.estimate();
		est.saveSchedule();
	}
	
	public static final int TRIP_MAX=30*60;//trip threshold (s)
	public static final int STOP_MAX=5*60;//stop threshold (s)

	private List<String> schedule;//<line_vehicle_station_time>

	private HashMap<String, List<String>> lineSchedule;

	private HashMap<String, List<String>> entryOnlyLineDetail;

	private boolean isScheduleGenraled = false;
	
	private boolean isScheduleFixed=false;

	public void estimate() {
		CardBean card = null;
		// TODO:card read in
		this.studyCard(card);
	}

	public void saveSchedule() {
		if (!isScheduleGenraled) {
			generalSchedule();
		}
		if(isScheduleFixed){
			fixSchedule();
		}
		// TODO: save the schedule
	}

	private void studyCard(CardBean card) {
		if (card.isEntryOnly()) {
			// entry only model
			String line = card.getLineId();
			String time = card.getUpTime();
			String vehicle = card.getVehicleId();
			addEntryOnlyDetail(line, vehicle, time);
		} else {
			// distance model
			String line = card.getLineId();
			String time = card.getDownTime();
			String sta = card.getDownSeq();
			String vehicle = card.getVehicleId();
			updateDistanceSchedule(line, vehicle, time, sta);
		}
	}

	private void addEntryOnlyDetail(String line, String vehicle, String time) {
		// add the detail to the entry only detail list
		if (entryOnlyLineDetail == null) {
			entryOnlyLineDetail = new HashMap<String, List<String>>();
		}
		String key = line + "_" + vehicle;
		if (!entryOnlyLineDetail.containsKey(key)) {
			entryOnlyLineDetail.put(key, new ArrayList<String>());
		}
		entryOnlyLineDetail.get(key).add(time);
	}

	private void updateDistanceSchedule(String line, String vehicle,
			String time, String station) {
		// update the distance model line schedule
		if (lineSchedule == null) {
			lineSchedule = new HashMap<String, List<String>>();
		}
		String key = line + "_" + vehicle + "_" + station;
		if (!lineSchedule.containsKey(key)) {
			lineSchedule.put(key, new ArrayList<String>());
		}
		lineSchedule.get(key).add(time);
	}

	public void generalSchedule() {
		// general the entry only line schedule by detail
		schedule = new ArrayList<String>();
		if (entryOnlyLineDetail != null && !entryOnlyLineDetail.isEmpty()) {
			// entry only line schedule build
			for (String key : entryOnlyLineDetail.keySet()) {
				List<String> times = entryOnlyLineDetail.get(key);
				Collections.sort(times, new Comparator<String>() {
					@Override
					public int compare(String o1, String o2) {
						try {
							return Integer.parseInt(o1) - Integer.parseInt(o2);
						} catch (Exception e) {
						}
						return 0;
					}
				});
				String oldtime=null;
				String timestamp="";
				int stationstamp=1;
				int sum=0;
				int num=0;
				for (String time:times){
					if (oldtime==null){
						oldtime=time;
						continue;
					}
					try {
						int range= Integer.parseInt(time) - Integer.parseInt(oldtime);
						if (range>TRIP_MAX){
							String newkey=key+"_"+stationstamp;
							if (num > 0) {
								sum /= num;
							}
							timestamp = Integer.toString(sum);
							schedule.add(newkey+"_"+timestamp);
							sum=0;num=0;
							stationstamp=1;
						}else if (range>STOP_MAX){
							String newkey=key+"_"+stationstamp;
							if (num > 0) {
								sum /= num;
							}
							timestamp = Integer.toString(sum);
							schedule.add(newkey+"_"+timestamp);
							sum=0;num=0;
							stationstamp++;
						}else{
							sum += Integer.parseInt(time);
							num++;
						}
					} catch (Exception e) {
						System.err.println("time format error:" + time
								+ " or " + oldtime);
						continue;
					}
					oldtime=time;
				}
			}
		}
		if (lineSchedule != null && !lineSchedule.isEmpty()) {
			// common schedule build
			for (String key : lineSchedule.keySet()) {
				List<String> times = lineSchedule.get(key);
				String timestamp = "";
				int sum = 0;
				int num = 0;
				for (String time : times) {
					// average the times
					try {
						sum += Integer.parseInt(time);
						num++;
					} catch (Exception e) {
						System.err.println("time format error:" + time);
						continue;
					}
				}
				if (num > 0) {
					sum /= num;
				}
				timestamp = Integer.toString(sum);
				schedule.add(key+"_"+timestamp);
			}
		}
		isScheduleGenraled = true;
	}
	
	public void fixSchedule(){
		//TODO:fix the schedule;
		isScheduleFixed=true;
	}
}
