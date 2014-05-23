package nlsde.buaa.inmem.ml;

import java.util.ArrayList;
import java.util.List;

import nlsde.buaa.inmem.ml.FastPassengerCount.KeyByLocationTimeUP;
import nlsde.buaa.inmem.ml.FastRegionChart.GeneralCharts;
import nlsde.buaa.inmem.ml.FastRegionDivide.GeneralRegion;
import nlsde.buaa.inmem.ml.FastRegionDivide.KeyByTime;
import nlsde.buaa.inmem.ml.FastRegionDivide.Map2RDD;
import nlsde.buaa.inmem.ml.ODExtraction.ODAtRandom;
import nlsde.buaa.inmem.ml.ODExtraction.RemoveErrorDate;
import nlsde.buaa.inmem.model.DataPool;
import nlsde.buaa.region.RegionDivide;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class AllInOne implements Runnable {
	
	private JavaRDD<String> SMARTCARD;
	private JavaRDD<String> STATION_INFO;
	
	private static JavaPairRDD<String, List<String>> POINT_COUNT;
	
	@SuppressWarnings("unchecked")
	public AllInOne(RegionDivide rd) {
		this.SMARTCARD = (JavaRDD<String>) DataPool.getInstance().get(this.getClass().getName(), "SMARTCARD");
		this.STATION_INFO = (JavaRDD<String>) DataPool.getInstance().get(this.getClass().getName(), "STATION_INFO");
		ODExtraction.bc_station_info = (ArrayList<String>) DataPool.getInstance().broadcast(this.STATION_INFO.collect());
		
		FastRegionDivide.rd = rd;
		FastRegionChart.rd = rd;
	}

	@Override
	public void run() {
		AllInOne.POINT_COUNT = 
		this.SMARTCARD
				.filter(new RemoveErrorDate())
				.map(new ODAtRandom())
				.keyBy(new KeyByLocationTimeUP())
                .groupByKey();
                
		
		
        AllInOne.POINT_COUNT.map(new Map2RDD())
						.keyBy(new KeyByTime())
						.groupByKey()
						.foreach(new GeneralRegion());
        
	}
	
	public static void main(String[] args) throws InterruptedException {
		DataPool.getInstance().loadAll();
		
		RegionDivide rd = new RegionDivide();
		
		double time;
		
		Thread aio = new Thread(new AllInOne(rd));
		time = System.currentTimeMillis();
		aio.start();
		aio.join();
		System.out.println("AllInOne took " + (System.currentTimeMillis() - time) / 1000 + " s.");
		
		rd.outTmpFile();
		rd.outAreaFile();
		
		POINT_COUNT.foreach(new GeneralCharts());
		
		rd.outChartFile();
	}

}
