package nlsde.buaa.stationpassenger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import nlsde.buaa.base.util.OutToFile;

public class PassengerCount {
	public static void main(String[] args){
		PassengerCount pc = new PassengerCount("20120709");
		pc.count();
		pc.outTmpFile();
		pc.outHeatFile();
	}
	public static final int HEAT_MAX=300;
	
	private final static String OUT_PATH="stationPassenger";
	private final static String SERVICE_PATH="webapp/data";

	public HashMap<String, PointCountBean> result;
	private String date;
	
	public PassengerCount(String date) {
		result = new HashMap<String, PointCountBean>();
		this.date=date;
	}
	
	public PassengerCount() {
		result = new HashMap<String, PointCountBean>();
		this.date = "20120830";
	}

	public void count() {
		String[] dates ={date};
		CardReadIn in = new CardReadIn();
		in.setCardDate(dates);

		int num = 0;
		CardBean card;
		while ((card = in.getCardInfo()) != null) {
			num++;
			if (num % 100000 == 0)
				System.out.println(num + ":" + System.currentTimeMillis());
			if (card.isAFCUp() || !card.isAvilable()) {
				continue;
			}
			this.add(card);
		}
	}

	private void add(CardBean card) {
		PointCountBean pcbUp=new PointCountBean(card,PointCountBean.UP);
		String key1 = pcbUp.getKey();
		if (!result.containsKey(key1)){
			result.put(key1, pcbUp);
		}
		result.get(key1).addOne();
		
		PointCountBean pcbDown=new PointCountBean(card,PointCountBean.DOWN);
		String key2 = pcbDown.getKey();
		if (!result.containsKey(key2)){
			result.put(key2, pcbDown);
		}
		result.get(key2).addOne();
	}
	
	private static <T> void outTofile(List<T> list, String filename) {
		// output the result to file
		OutToFile.outToFile(list, filename);
	}
	
	public void outTmpFile(){
		List<PointCountBean> list=new ArrayList<PointCountBean>();
		for (String key:result.keySet()){
			list.add(result.get(key));
		}
		outTofile(list,OUT_PATH+File.separator+date+".csv");
	}
	
	public void outHeatFile(){
		HashMap<String,List<PointCountBean>> tmap=new HashMap<String,List<PointCountBean>>();
		for (String key:result.keySet()){
			PointCountBean pb=result.get(key);
			String time=pb.getTime()+"_"+pb.getUd();
			if (!tmap.containsKey(time)){
				tmap.put(time, new ArrayList<PointCountBean>());
			}
			tmap.get(time).add(pb);
		}
		for(String key:tmap.keySet()){
			String[] subkey=key.split("_");
			if (Integer.valueOf(subkey[0].substring(8)) < 10) {
				subkey[0] = subkey[0].substring(9);
			} else {
				subkey[0] = subkey[0].substring(8);
			}
			outToHeatFile(tmap.get(key),SERVICE_PATH+File.separator+"heat"+File.separator+subkey[1]+File.separator+formatDate(date)+File.separator+subkey[0]+".json");
		}
	}
	private String formatDate(String date){
		return date.substring(0,4)+"-"+date.substring(4,6)+"-"+date.substring(6,8);
	}
	
	private static <T> void outToHeatFile(List<PointCountBean> list, String outfile) {
		OutToFile.mkdir(outfile, false);
		try {
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream(outfile), "gbk"), true);
			pw.println("{\"max\":"+HEAT_MAX+",\"data\":[");
			boolean first=true;
			for (PointCountBean t : list) {
				if (first){
					first=false;
					pw.println(t.getHeatString());
				}else{
					pw.println(","+t.getHeatString());
				}
			}
			pw.println("]}");
			pw.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
}
