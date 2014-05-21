package nlsde.buaa.boot;

import nlsde.buaa.base.util.CONSTANT;
import nlsde.buaa.groupmobility.StreamCount;
import nlsde.buaa.region.RegionDivide;
import nlsde.buaa.stationpassenger.PassengerCount;

public class PassengerAnalysis {

	public static void main(String[] args) {
		//也可以通过读入参数实现自动计算
		//String date=args[0];
		CONSTANT.CardType=CONSTANT.BUS;
//		String date="20120709";
		String date="20120910";
		startAnalaysis(date);
	}

	public static void startAnalaysis(String date) {
		System.out.println(System.currentTimeMillis()+"::Analaysis start::"+date);
		// 计算站点客流
		System.out.println(System.currentTimeMillis()+"::start stationCount::"+date);
		PassengerCount pc = new PassengerCount(date);
		pc.count();
		pc.outTmpFile();
		pc.outHeatFile();

		// 计算区域划分及聚集客流
		System.out.println(System.currentTimeMillis()+"::start RegionCount::"+date);
		RegionDivide rd = new RegionDivide(date);
		rd.generalTheRegion();
		rd.outTmpFile();
		rd.outAreaFile();
		// out file before general chart for building region edge
		rd.generalTheChart();
		rd.outChartFile();

		// 计算流动客流
		System.out.println(System.currentTimeMillis()+"::start streamCount::"+date);
		StreamCount sc = new StreamCount(date);
		sc.countStream();
		sc.outTmpFile();
		sc.outStreamFile();
		sc.outChartFile();
		
		System.out.println(System.currentTimeMillis()+"::Analaysis finished::"+date);
	}
}
