package nlsde.buaa.patternlibrary;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class PatternStudy {

	public static void main(String[] args) {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date sd = new Date();
		System.out.println("start time:" + df.format(sd));
		String[] date = { "20120709", "20120710", "20120711", "20120712",
				"20120713", "20120716", "20120717", "20120718", "20120719",
				"20120723", "20120724", "20120725", "20120726", "20120727" };
//		String[] date = { "20120709"};
		CardReadIn in=new CardReadIn();
		in.setCardDate(date);
		PatternStudy ps=new PatternStudy();
		
		int num=0;
		CardBean card;
		while ((card = in.getCardInfo()) != null) {
			num++;
			if (num%10000==0)
				System.out.println(num+":"+df.format(new Date()));
			if (card.isAFCUp()||!card.isAvilable()){
				continue;
			}
			ps.study(card);
		}
		
		Date ed=new Date();
		System.out.println("start time:"+df.format(sd));
		System.out.println("end time:"+df.format(ed));
	}

	private static final int DST = 500;// 空间合并阈值，m
	private static final int TST = 60;// 时间间合并阈值，m

	private IPatternService ips;

	public PatternStudy() {
		ips = ServiceFatory.getPatternService();
	}

	/**
	 * 模式学习，对于每条一卡通数据(上车信息完整)
	 * 
	 * @param card
	 */
	public void study(CardBean card) {
		PatternBean pb = PatternBean.getPattern(card);
		PassengerPattern pp = ips.getPattern(card.getCardID());
		List<PatternBean> pbs = pp.getPatternList();
		List<PatternBean> npbs = study(pbs, pb);
		pp.updatePattern(npbs);
		if (pp.isNew()) {
			ips.savePattern(pp);
		} else {
			ips.updatePattern(pp);
		}

	}

	private List<PatternBean> study(List<PatternBean> pbs, PatternBean pb) {
		if (pbs == null || pbs.size() == 0) {
			pbs = new ArrayList<PatternBean>();
			pbs.add(pb);
			return pbs;
		}
		List<PatternBean> npbs = new ArrayList<PatternBean>();
		boolean mf=false;
		for (PatternBean tpb : pbs) {
			if (inTimeScale(tpb, pb)) {
				if (inSpaceScale(tpb, pb)) {
					PatternBean npb = merge(tpb, pb);
					npbs.add(npb);
					mf=true;
					continue;
				}
			}
			npbs.add(tpb);
		}
		if (!mf){
			npbs.add(pb);
		}
		return npbs;
	}

	private PatternBean merge(PatternBean pb1, PatternBean pb2) {
		double time = ((pb1.getTime() + pb2.getTime()) / 2);
		double lon = ((pb1.getLon() + pb2.getLon()) / 2);
		double lat = ((pb1.getLat() + pb2.getLat()) / 2);
		int weight = pb1.getWeight() + pb2.getWeight();
		PatternBean pb = new PatternBean(time, lon, lat, weight);
		return pb;
	}

	private boolean inTimeScale(PatternBean pb1, PatternBean pb2) {
		if (time(pb1.getTime(), pb2.getTime()) < TST) {
			return true;
		}
		return false;
	}

	private boolean inSpaceScale(PatternBean pb1, PatternBean pb2) {
		if (distence(pb1.getLon(), pb1.getLat(), pb2.getLon(), pb2.getLat()) < DST) {
			return true;
		}
		return false;
	}

	private double distence(double x1, double y1, double x2, double y2) {
		return Math.sqrt(Math.pow(
				111000 * Math.abs(x1 - x2)
						* Math.cos((y1 + y2) / 2 * Math.PI / 180), 2)
				+ Math.pow(111000 * Math.abs(y1 - y2), 2));
	}

	private double time(double t1, double t2) {
		return Math.abs(t1 - t2);
	}

}
