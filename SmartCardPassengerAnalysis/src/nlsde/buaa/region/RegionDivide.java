package nlsde.buaa.region;

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

public class RegionDivide {
	public static void main(String[] args) {
		CONSTANT.CardType=CONSTANT.BUS;
		RegionDivide rd = new RegionDivide("20120910");
		rd.generalTheRegion();
		rd.outTmpFile();
		rd.outAreaFile();
		// out file before general chart for building region edge
		rd.generalTheChart();
		rd.outChartFile();
	}
	public static final int AVILABLE_TH=5;

//	public static final int COUNT_TH = 4000;
//	public static final int DISTENCE_TH = 1500;
//	public static final int SIMILAR_TH = 50;
	public static final int COUNT_TH = 300;
	public static final int DISTENCE_TH = 500;
	public static final int SIMILAR_TH = 50;
	
	// TODO for spark local test
//	public static final int COUNT_TH = 2;
//	public static final int DISTENCE_TH = 500;
//	public static final int SIMILAR_TH = 50;

	private final static String OUT_PATH = "regionCount";
	private final static String SERVICE_PATH = "webapp/data";

	private String date;
	public HashMap<String, List<RegionCountBean>> result = null;

	public RegionDivide(String date) {
		this.date = date;
	}
	
	public RegionDivide () {
		this.date = "20120830";
	}

	public void generalTheRegion() {
		PointCountReadIn in = new PointCountReadIn();
		in.setDate(date);
		HashMap<String, List<PointCountBean>> map = new HashMap<String, List<PointCountBean>>();
		PointCountBean pcb = null;
		int num=0;
		while ((pcb = in.getPointCountBean()) != null) {
			if (pcb.getCount()<=AVILABLE_TH)continue;
			num++;
			String key = pcb.getTime() + "_" + pcb.getUd();
			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<PointCountBean>());
			}
			map.get(key).add(pcb);
		}
		System.out.println("station read in: "+num);
		num=0;
		for (String time : map.keySet()) {
			num++;
			List<PointCountBean> list=map.get(time);
			System.out.println("finished: "+(num*100/map.size())+"% deal key: "+time+" with station: "+list.size());
			this.generalTheRegion(time,list );
		}
		System.out.println("region general finished");
	}

	public void generalTheChart() {
		System.out.println("start general chart:" + System.currentTimeMillis());
		PointCountReadIn in = new PointCountReadIn();
		in.setDate(date);
		PointCountBean pcb = null;
		int num=0;
		while ((pcb = in.getPointCountBean()) != null) {
			num++;
			if (num % 10000 == 0)
				System.out.println(num + ":" + System.currentTimeMillis());
			for (String key : result.keySet()) {
				List<RegionCountBean> tl = result.get(key);
				for (RegionCountBean rcb : tl) {
					if (RegionUtil.pointInRegion(pcb, rcb)) {
						rcb.addChartCount(pcb.getTime(), pcb.getUd(),
								pcb.getCount());
					}
				}
			}
		}
	}
	
	public void generalTheChart(PointCountBean pcb) {
		for (String key : result.keySet()) {
			List<RegionCountBean> tl = result.get(key);
			for (RegionCountBean rcb : tl) {
				if (RegionUtil.pointInRegion(pcb, rcb)) {
					rcb.addChartCount(pcb.getTime(), pcb.getUd(),
							pcb.getCount());
				}
			}
		}
	}

	public void generalTheRegion(String time, List<PointCountBean> list) {
		List<RegionCountBean> re = new ArrayList<RegionCountBean>();
		boolean flag=true;
		while (flag){
			flag=false;
			RegionCountBean rcb=null;
			for (PointCountBean pcb : list) {
				if (pcb.dealed)
					continue;
				if (rcb==null){
					rcb=new RegionCountBean();
					merge(rcb, pcb);
					pcb.dealed=true;
					flag=true;
				}
				else if (canMerge(rcb,pcb)){
					merge(rcb, pcb);
					pcb.dealed=true;
					flag=true;
				}
			}
			if (flag){
				if (rcb.getCount()>COUNT_TH){
					re.add(rcb);
//					rcb.toString();
				}
			}
		}
//		for (PointCountBean pcb : list) {
//			num++;
//			this.merge(re, pcb);
//			if (num%500==0){
//				System.out.println("merge num: "+num+" cur regions: "+re.size());
//			}
//		}
		System.out.println("merge regions: "+re.size());
		if (result == null) {
			result = new HashMap<String, List<RegionCountBean>>();
		}
		result.put(time, re);
	}

//	private void merge(List<RegionCountBean> list, PointCountBean pcb) {
//		// merge into the existing region result
//		boolean merged = false;
//		for (RegionCountBean r : list) {
//			if (canMerge(r, pcb)) {
//				merge(r, pcb);
//				merged = true;
//			}
//		}
//		if (!merged) {
//			RegionCountBean nr = new RegionCountBean();
//			merge(nr, pcb);
//			list.add(nr);
//		}
//	}

	private void merge(RegionCountBean r, PointCountBean p) {
		r.addStation(p);
		r.addCount(p.getCount());
	}

	private boolean canMerge(RegionCountBean r, PointCountBean p) {
		if (r.getCount() + p.getCount() < COUNT_TH) {
			return false;
		}
		if (station2stationsDistence(r, p) > DISTENCE_TH) {
			return false;
		}
		if (RegionUtil.point2RegionSimilar(p, r) < SIMILAR_TH) {
			return false;
		}
		return true;
	}

	private double station2stationsDistence(RegionCountBean r, PointCountBean p) {
		double mind = -1;
		for (PointBean dp : r.stations) {
			if (mind < 0) {
				mind = RegionUtil.distence(dp, p);
			} else {
				double cd = RegionUtil.distence(dp, p);
				if (cd < mind) {
					mind = cd;
				}
			}
		}
		return mind > 0 ? mind : 0;
	}

	public void outTmpFile() {
		List<String> list = new ArrayList<String>();
		for (String key : result.keySet()) {
			String[] subkey = key.split("_");
			List<RegionCountBean> tpl = result.get(key);
			for (RegionCountBean rcb : tpl) {
				list.add(subkey[0] + "," + subkey[1] + "," + rcb.toString());
			}
		}
		OutToFile
		.outToFile(list, OUT_PATH + File.separator + date + ".csv");
	}

	public void outAreaFile() {
		for (String key : result.keySet()) {
			String[] subkey = key.split("_");
			if (Integer.valueOf(subkey[0].substring(8)) < 10) {
				subkey[0] = subkey[0].substring(9);
			} else {
				subkey[0] = subkey[0].substring(8);
			}
			outToHeatFile(result.get(key), SERVICE_PATH + File.separator
					+ "area" + File.separator + subkey[1] + File.separator
					+ formatDate(date) + File.separator + subkey[0] + ".json");
		}
	}

	public void outChartFile() {
		for (String key : result.keySet()) {
			String[] subkey = key.split("_");
			if (Integer.valueOf(subkey[0].substring(8)) < 10) {
				subkey[0] = subkey[0].substring(9);
			} else {
				subkey[0] = subkey[0].substring(8);
			}
			List<RegionCountBean> tpl = result.get(key);
			int seq = 0;
			for (RegionCountBean rcb : tpl) {
				String filename = SERVICE_PATH + File.separator + "chart"
						+ File.separator + subkey[1] + File.separator + formatDate(date) 
						+ File.separator + subkey[0] + File.separator + "are_"
						+ seq + ".csv";
				outToChartFile(rcb, filename);
				seq++;
			}
		}
	}

	private String formatDate(String date) {
		return date.substring(0, 4) + "-" + date.substring(4, 6) + "-"
				+ date.substring(6, 8);
	}

	private static <T> void outToHeatFile(List<RegionCountBean> list,
			String outfile) {
		OutToFile.mkdir(outfile, false);
		try {
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream(outfile), "gbk"), true);
			pw.println("[");
			boolean first = true;
			for (RegionCountBean r : list) {
				if (r.getCount() < COUNT_TH)
					continue;
				if (first) {
					first = false;
				} else {
					pw.println(",");
				}
				pw.println("[" + "[" + r.getRegionEdgeString() + "],"
						+ r.getCount() + "]");
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

	private static <T> void outToChartFile(RegionCountBean rb, String outfile) {
		OutToFile.mkdir(outfile, false);
		try {
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream(outfile), "utf-8"), true);
			pw.println("time,下车,上车");
			if (rb.chartlist != null) {
				for (String key : rb.chartlist.keySet()) {
					pw.println(rb.chartlist.get(key));
				}
			}else{
				System.out.println(rb.points);
				System.out.println(rb.stations);
				System.out.println(rb.getCount());
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
