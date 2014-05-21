package nlsde.buaa.groupmobility;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import nlsde.buaa.base.util.CONSTANT;
import nlsde.buaa.base.util.OutToFile;

public class StreamCount {
	public static void main(String[] args) {
		CONSTANT.CardType=CONSTANT.BUS;
		StreamCount sc = new StreamCount("20120910");
		sc.countStream();
		sc.outTmpFile();
		sc.outStreamFile();
		sc.outChartFile();
	}

	private final static int STREAM_TH = 100;

	private final static String OUT_PATH = "streamCount";
	private final static String SERVICE_PATH = "webapp/data";

	private String date;
	private HashMap<String, StreamBean> result = null;
	private HashMap<String, List<StreamBean>> finalResult=null;
	private List<RegionCountBean> uRegions;
	private List<RegionCountBean> dRegions;

	public StreamCount(String date) {
		this.date = date;
	}

	public void countStream() {
		this.readInRegion();
		String[] dates = { date };
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
		this.buildFinalResult();
	}

	private void readInRegion() {
		uRegions = new ArrayList<RegionCountBean>();
		dRegions = new ArrayList<RegionCountBean>();
		RegionCountReadIn in = new RegionCountReadIn();
		in.setDate(date);
		RegionCountBean rcb;
		while ((rcb = in.getRegionCountBean()) != null) {
			switch (rcb.getUD()) {
			case RegionCountBean.UP:
				uRegions.add(rcb);
				break;
			case RegionCountBean.DOWN:
				dRegions.add(rcb);
				break;
			}
		}
		System.out.println("up regions: " +uRegions.size()+" down regions: "+dRegions.size());
	}

	private void add(CardBean card) {
		if (result == null)
			result = new HashMap<String, StreamBean>();
		PointBean uppoint = new PointBean(Double.parseDouble(card.getUpLon()),
				Double.parseDouble(card.getUpLat()));
		List<RegionCountBean> upregions = new ArrayList<RegionCountBean>();
		if (uRegions != null) {
			for (RegionCountBean rcb : uRegions) {
				if (RegionUtil.pointInRegion(uppoint, rcb)) {
					upregions.add(rcb);
				}
			}
		}
		if (upregions.size() <= 0)
			return;
		PointBean downpoint = new PointBean(Double.parseDouble(card
				.getDownLon()), Double.parseDouble(card.getDownLat()));
		List<RegionCountBean> downregions = new ArrayList<RegionCountBean>();
		if (dRegions != null) {
			for (RegionCountBean rcb : dRegions) {
				if (RegionUtil.pointInRegion(downpoint, rcb)) {
					downregions.add(rcb);
				}
			}
		}
		if (downregions.size() <= 0)
			return;
		for (RegionCountBean upb : upregions) {
			for (RegionCountBean dpb : downregions) {
				if (upb.getTime().equals(dpb.getTime())) {
					int ct = Integer.parseInt(upb.getTime());
					// result key: time,up_point,down_point
					String key = ct + "," + upb + "," + dpb;

					if (!result.containsKey(key)) {
						result.put(key, new StreamBean(ct, upb.center,
								dpb.center));
					}
					String uptime = card.getUpTime(CardBean.HOUR);
					int ut = Integer.parseInt(uptime);
					result.get(key).addOne(ut);
				}
			}
		}
	}
	
	private void buildFinalResult(){
		finalResult = new HashMap<String, List<StreamBean>>();
		for (String key : result.keySet()) {
			if (result.get(key).getCount() < STREAM_TH) {
				continue;
			}
			String nk = key.split(",")[0];
			if (!finalResult.containsKey(nk)) {
				finalResult.put(nk, new ArrayList<StreamBean>());
			}
			finalResult.get(nk).add(result.get(key));
		}
	}

	public void outTmpFile() {
		List<StreamBean> list = new ArrayList<StreamBean>();
		for (String key : result.keySet()) {
			list.add(result.get(key));
		}
		OutToFile.outToFile(list, OUT_PATH + File.separator + date + ".csv");
	}

	public void outStreamFile() {
		for (String time : finalResult.keySet()) {
			this.outStreamFile(finalResult.get(time), SERVICE_PATH + File.separator
					+ "stream" + File.separator + formatDate(date)
					+ File.separator + time + ".json");
		}
	}
	
	public void outChartFile(){
		for (String key : finalResult.keySet()) {
			List<StreamBean> tpl = finalResult.get(key);
			int seq = 0;
			for (StreamBean sb : tpl) {
				String filename = SERVICE_PATH + File.separator + "chart"
						+ File.separator + "1"+ File.separator + formatDate(date) 
						+ File.separator + key + File.separator + "arr_"
						+ seq + ".csv";
				outChartFile(sb, filename);
				seq++;
			}
		}
	}

	private String formatDate(String date) {
		return date.substring(0, 4) + "-" + date.substring(4, 6) + "-"
				+ date.substring(6, 8);
	}

	private void outStreamFile(List<StreamBean> list, String filename) {
		OutToFile.mkdir(filename, false);
		try {
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream(filename), "gbk"), true);
			pw.println("[");
			boolean first = true;
			for (StreamBean sb : list) {
				if (!first) {
					pw.println(",");
				}
				first = false;
				pw.println("[" + sb.toStreamString() + "]");
			}
			pw.println("]");
			pw.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	private void outChartFile(StreamBean sb, String filename) {
		OutToFile.mkdir(filename, false);
		try {
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream(filename), "utf-8"), true);
			pw.println("time,下车,上车");
			if (sb.chartlist != null) {
				for (Integer key : sb.chartlist.keySet()) {
					pw.println(sb.chartlist.get(key));
				}
			}else{
				System.out.println(sb.toStreamString());
			}
			pw.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
}
