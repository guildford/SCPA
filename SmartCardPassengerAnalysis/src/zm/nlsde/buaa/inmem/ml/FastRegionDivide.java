package zm.nlsde.buaa.inmem.ml;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import yy.nlsde.buaa.region.PointCountBean;
import yy.nlsde.buaa.region.RegionCountBean;
import yy.nlsde.buaa.region.RegionDivide;
import yy.nlsde.buaa.region.RegionUtil;
import zm.nlsde.buaa.inmem.model.DataPool;

public class FastRegionDivide implements Runnable{
	
	private JavaPairRDD<String, List<String>> POINT_COUNT;
	
	private static RegionDivide rd = new RegionDivide();
	
	@SuppressWarnings("unchecked")
	public FastRegionDivide() {
		this.POINT_COUNT = (JavaPairRDD<String, List<String>>) DataPool.getInstance().get(this.getClass().getName(), "POINT_COUNT", true);
	}

	@Override
	public void run() {
//		this.POINT_COUNT
//		// map pairRDD to RDD so that new key can be set
//		.map(new Map2RDD())
//		// key by time-stamp concatenated with Up or Down
//		.keyBy(new KeyByTime())
//		.groupByKey()
//		// if count is less than a certain value, then the record is regarded as dirty.
//		.filter(new RemoveDirty());
		
		this.POINT_COUNT
		.map(new Map2RDD())
		.keyBy(new KeyByTime())
		.groupByKey()
		.foreach(new GeneralRegion());
		
		this.POINT_COUNT
		.foreach(new GeneralCharts());
		
	}

	@SuppressWarnings("serial")
	static class Map2RDD extends Function<Tuple2<String, List<String>>, String> {

		@Override
		public String call(Tuple2<String, List<String>> tp)
				throws Exception {
			return tp._1() + "," + tp._2().size();
		}
		
	}
	
	@SuppressWarnings("serial")
	static class KeyByTime extends Function<String, String> {

		@Override
		public String call(String ss) throws Exception {
			String[] token = ss.split(",");
			return token[2] + "#" + token[3];
		}
		
	}
	
	@SuppressWarnings("serial")
	static class GeneralRegion extends VoidFunction<Tuple2<String, List<String>>> {

		@Override
		public void call(Tuple2<String, List<String>> tp) throws Exception {
			List<PointCountBean> list = new ArrayList<PointCountBean>();
			for (String rec: tp._2()) {
				list.add(new PointCountBean(rec));
			}
			rd.generalTheRegion(tp._1().split("#")[0], list);
		}
		
	}
	
	@SuppressWarnings("serial")
	static class GeneralCharts extends VoidFunction<Tuple2<String, List<String>>> {
		RegionUtil ru = null;

		@Override
		public void call(Tuple2<String, List<String>> tp) throws Exception {
//			if (tp._2().size() > 3) {
//				System.out.println(tp._1() + "*********" + tp._2().size());
//			}
			rd.generalTheChart(new PointCountBean(tp._1() + "," + tp._2().size()));
		}
		
	}
	
}
