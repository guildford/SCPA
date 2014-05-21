package nlsde.buaa.estimate.down;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import nlsde.buaa.base.util.OutToFile;

public class Evaluation {
	
	public static long timestamp;
	public static void main(String[] args) {
		System.out.println("start time:" + (timestamp=System.currentTimeMillis()));
//		String[] dates = { "20120709", "20120710", "20120711", "20120712",
//				"20120713", "20120716", "20120717", "20120718", "20120719",
//				"20120723", "20120724", "20120725", "20120726", "20120727" };
		String[] dates={"20120727"};
		List<String> rl = new ArrayList<String>();
		for (String date : dates) {
			Evaluation e = new Evaluation();
			e.evaluate(date);
			rl.add(e.getResult());
		}
		Evaluation.outTofile(
				rl,
				"downEvaluateResult" + File.separator
						+ System.currentTimeMillis() + ".csv");
		System.out.println("start time:" + System.currentTimeMillis());
		System.out.println("end time:" + System.currentTimeMillis());
	}

	public static <T> void outTofile(List<T> list, String filename) {
		// output the result to file
		OutToFile.outToFile(list, filename);
	}

	private final int TOTAL_NUM = 0;
	private final int NO_PATTERN = 1;// 无此人模式
	private final int MISS_PATTERN = 2;// 无对应时段模式		//21.32%
	private final int WRONG_PATTERN = 3;// 对应时段模式匹配错误	//29.86%
	private final int CORRECT_PATTERN = 4;// 对应时段模式匹配正确		//48.81%
	private final int CORRECT_DOWN = 5;// 下车位置匹配正确	//22.28%
	private final int WRONG_DOWN = 6;// 下车位置匹配错误		//56.39%
									//78.67%

	//下车匹配过程中相关参数
	private final int DIRECT_MATCH = 7;// 直接匹配成功	//51.47%
	private final int MORNING_MATCH = 8;// 最后一次乘车，以早上模式匹配	//23.61%
	private final int TEMP_MATCH = 9;// 降低条件后匹配	//24.91%
	private final int TEMPMORNING_MATCH = 10;// 降低条件后早上匹配
	private final int NODOWN_CHOOSE = 11;// 没有下车匹配选择

	// 2013.8.1:
	//[101072, 40078, 19232, 37493, 4269, 7222, 34540]
	// 2012.8.2:DST=1000 TST=2
	//[40220, 12607, 5888, 8246, 13479, 6154, 15571, 11183, 5129, 5413, 0, 0]
	/*2012.8.5
	6450000:1s
	result at DST=5000 and TST=3:
	TOTAL_NUM:60785
	NO_PATTERN:16468	27.092210249239123%
	MISS_PATTERN:5955	9.796824874557867%
	WRONG_PATTERN:10689	17.584930492720243%
	CORRECT_PATTERN:27673	45.52603438348277%
	CORRECT_DOWN:20165	45.50172619987815%
	WRONG_DOWN:24152	54.49827380012186%
	DIRECT_MATCH:17801	40.16743010582846%
	MORNING_MATCH:15287	34.49466344743552%
	TEMP_MATCH:16468	37.15955502403141%
	TEMPMORNING_MATCH:5955	13.437281404427196%
	NODOWN_CHOOSE:0	0.0%
	********************************************
	6450000:1s
	result at DST=10000 and TST=4:
	TOTAL_NUM:60902
	NO_PATTERN:16306	26.774161768086437%
	MISS_PATTERN:4237	6.957078585268135%
	WRONG_PATTERN:6908	11.342813043906604%
	CORRECT_PATTERN:33451	54.925946602738826%
	CORRECT_DOWN:29407	65.94089155978115%
	WRONG_DOWN:15189	34.05910844021886%
	DIRECT_MATCH:19368	43.42990402726702%
	MORNING_MATCH:18528	41.546327024845276%
	TEMP_MATCH:2463	5.522916853529464%
	TEMPMORNING_MATCH:4237	9.500852094358239%
	NODOWN_CHOOSE:0	0.0%
	********************************************
	
	*/

	/**
	 * 关于最终评价的说明： </br>
	 * 1.总数以TOTAL_NUM-NO_PATTERN计，因为学习的时期和实验评价的时期不同</br> 
	 * 2.WRONG_PATTERN代表偶发出行量，如果以分钟为单位结果会更好</br>
	 * 3.理论上通过修改下车推测方法的参数，可以实现CORRECT_PATTERN的推测量，甚至更多</br>
	 * 4.下车匹配过程中参数有利于调整下车匹配方法</br>
	 * 5.NODOWN_CHOOSE是肯定推不出来的</br>
	 */

	private int[] result;

	private IPatternService ips;

	//TODO:修改阈值实验
	private static final int DST = 10000;// 相同空间判定阈值，m
	// TODO:临时改成小时
	private static final int TST = 4;// 乘车时间范围阈值，m

	public Evaluation() {
		ips = ServiceFatory.getPatternService();
		result = new int[12];
		result[NO_PATTERN] = 0;
		result[MISS_PATTERN] = 0;
		result[WRONG_PATTERN] = 0;
		result[CORRECT_PATTERN] = 0;
		result[CORRECT_DOWN] = 0;
		result[WRONG_DOWN] = 0;
		result[TOTAL_NUM] = 0;

		result[DIRECT_MATCH] = 0;
		result[MORNING_MATCH] = 0;
		result[TEMP_MATCH] = 0;
		result[TEMPMORNING_MATCH] = 0;
		result[NODOWN_CHOOSE] = 0;
	}

	public String getResult() {
		return result[TOTAL_NUM] + "," + result[CORRECT_DOWN] + ","
				+ result[WRONG_DOWN] + "," + result[CORRECT_PATTERN] + ","
				+ result[WRONG_PATTERN] + "," + result[MISS_PATTERN] + ","
				+ result[NO_PATTERN];
	}

	public void evaluate(String date) {
		String[] dates = { date };
		CardReadIn in = new CardReadIn();
		in.setCardDate(dates);

		int num = 0;
		int rand=0;
		CardBean card;
		while ((card = in.getCardInfo()) != null) {
			num++;
			if (num % 10000 == 0){
				long tt= System.currentTimeMillis();
				System.out.println(num + ":" +(tt-timestamp)/1000+"s");
				timestamp=tt;
				System.out.println("result at DST="+DST+" and TST="+TST+":");
				System.out.println("TOTAL_NUM:"+result[TOTAL_NUM]);
				System.out.println("NO_PATTERN:"+result[NO_PATTERN]+"\t"+result[NO_PATTERN]/(double)result[TOTAL_NUM]*100+"%");
				System.out.println("MISS_PATTERN:"+result[MISS_PATTERN]+"\t"+result[MISS_PATTERN]/(double)result[TOTAL_NUM]*100+"%");
				System.out.println("WRONG_PATTERN:"+result[WRONG_PATTERN]+"\t"+result[WRONG_PATTERN]/(double)result[TOTAL_NUM]*100+"%");
				System.out.println("CORRECT_PATTERN:"+result[CORRECT_PATTERN]+"\t"+result[CORRECT_PATTERN]/(double)result[TOTAL_NUM]*100+"%");
				System.out.println("CORRECT_DOWN:"+result[CORRECT_DOWN]+"\t"+result[CORRECT_DOWN]/(double)(result[TOTAL_NUM]-result[NO_PATTERN])*100+"%");
				System.out.println("WRONG_DOWN:"+result[WRONG_DOWN]+"\t"+result[WRONG_DOWN]/(double)(result[TOTAL_NUM]-result[NO_PATTERN])*100+"%");
				System.out.println("DIRECT_MATCH:"+result[DIRECT_MATCH]+"\t"+result[DIRECT_MATCH]/(double)(result[TOTAL_NUM]-result[NO_PATTERN])*100+"%");
				System.out.println("MORNING_MATCH:"+result[MORNING_MATCH]+"\t"+result[MORNING_MATCH]/(double)(result[TOTAL_NUM]-result[NO_PATTERN])*100+"%");
				System.out.println("TEMP_MATCH:"+result[TEMP_MATCH]+"\t"+result[TEMP_MATCH]/(double)(result[TOTAL_NUM]-result[NO_PATTERN])*100+"%");
				System.out.println("TEMPMORNING_MATCH:"+result[TEMPMORNING_MATCH]+"\t"+result[TEMPMORNING_MATCH]/(double)(result[TOTAL_NUM]-result[NO_PATTERN])*100+"%");
				System.out.println("NODOWN_CHOOSE:"+result[NODOWN_CHOOSE]+"\t"+result[NODOWN_CHOOSE]/(double)(result[TOTAL_NUM]-result[NO_PATTERN])*100+"%");
				System.out.println("********************************************");
			}
			if (num%100!=rand){
				continue;
			}
			rand=(int)( Math.random()*99);
			if (card.isAFCUp() || !card.isAvilable()) {
				continue;
			}
			this.evaluate(card);
		}
	}

	public void evaluate(CardBean card) {
		result[TOTAL_NUM]++;
		PassengerPattern pp = ips.getPattern(card.getCardID());
		List<PatternBean> pl = pp.getPatternList();
		if (pl == null || pl.size() <= 0) {
			result[NO_PATTERN]++;
			return;
		}
		PatternBean cp = getCorrectPattern(pl, card);
		if (cp != null) {
			if (isCorrectPattern(card, cp)) {
				result[CORRECT_PATTERN]++;
			} else {
				result[WRONG_PATTERN]++;
			}
		}
		if (isCorrectDown(card,pl)) {
			result[CORRECT_DOWN]++;
		} else {
			result[WRONG_DOWN]++;
		}
	}

	private PatternBean getCorrectPattern(List<PatternBean> pl, CardBean card) {
		// get the most probably pattern
		List<PatternBean> tl = new ArrayList<PatternBean>();
		PatternBean cpb = new PatternBean(card);
		for (PatternBean pb : pl) {
			if (inTimeScale(pb, cpb)) {
				// add to the property list
				tl.add(pb);
			}
		}
		if (tl.size() <= 0) {
			result[MISS_PATTERN]++;
			return null;
		}
		PatternBean rs = null;
		for (PatternBean pb : tl) {
			// TODO:bus card in line filter
			if (rs == null) {
				rs = pb;
			} else {
				if (rs.weight < pb.weight) {
					rs = pb;
				}
			}
		}
		return rs;
	}

	private boolean isCorrectPattern(CardBean card, PatternBean cp) {
		PatternBean tpb = new PatternBean(card);
		if (inSpaceScale(tpb, cp))
			return true;
		return false;
	}

	private boolean isCorrectDown(CardBean card,
			List<PatternBean> pl) {
		Collections.sort(pl, new Comparator<PatternBean>() {
			@Override
			public int compare(PatternBean o1, PatternBean o2) {
				return (int) (o1.getTime() - o2.getTime());
			}
		});
		PatternBean cpb = new PatternBean(card);
		boolean flag = false;
		int stweight = 0;
		List<PatternBean> tpl = new ArrayList<PatternBean>();
		for (PatternBean tp : pl) {
			if (inTimeScale(cpb, tp)) {
				flag = true;
			}
			if (flag) {
				if (inSpaceScale(cpb, tp)&&tp.getWeight()>stweight) {
					stweight = tp.getWeight();
				} else {
					tpl.add(tp);
				}
			}
		}
		// get the correct down pattern
		PatternBean corPb = null;
		for (PatternBean tp : tpl) {
			if (stweight > 0 && tp.getWeight() > stweight / 2) {
				result[DIRECT_MATCH]++;
				corPb = tp;
				break;
			}
		}
		// the last down need to find the morning up
		if (corPb == null) {
			for (PatternBean tp : pl) {
				if (stweight > 0 && tp.getWeight() > stweight / 2) {
					result[MORNING_MATCH]++;
					corPb = tp;
					break;
				}
			}
		}
		// low down the filter
		if (corPb == null && tpl.size() > 0) {
			result[TEMP_MATCH]++;
			corPb = tpl.get(0);
		}
		if (corPb == null && pl.size() > 0) {
			result[TEMPMORNING_MATCH]++;
			corPb = pl.get(0);
		}
		// no pattern can find
		if (corPb == null) {
			result[NODOWN_CHOOSE]++;
			return false;
		}
		return inSpaceScale(card, corPb);
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

	// down space
	private boolean inSpaceScale(CardBean card, PatternBean pb) {
		if (distence(Double.valueOf(card.getDownLon()),
				Double.valueOf(card.getDownLat()), pb.getLon(), pb.getLat()) < DST) {
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
