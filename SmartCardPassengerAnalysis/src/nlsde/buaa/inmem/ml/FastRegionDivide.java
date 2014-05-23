package nlsde.buaa.inmem.ml;

import java.util.ArrayList;
import java.util.List;

import nlsde.buaa.inmem.model.DataPool;
import nlsde.buaa.region.PointCountBean;
import nlsde.buaa.region.RegionDivide;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class FastRegionDivide implements Runnable{
	
	private JavaPairRDD<String, List<String>> POINT_COUNT;
	
	public static RegionDivide rd;
	
	@SuppressWarnings("unchecked")
	public FastRegionDivide(RegionDivide rd) {
		this.POINT_COUNT = (JavaPairRDD<String, List<String>>) DataPool.getInstance().get(this.getClass().getName(), "POINT_COUNT", true);
		FastRegionDivide.rd = rd;
	}

	@Override
	public void run() {
		
		this.POINT_COUNT
		// map pairRDD to RDD so that new key can be set
		.map(new Map2RDD())
		// key by time-stamp concatenated with Up or Down
		.keyBy(new KeyByTime())
		.groupByKey()
		.foreach(new GeneralRegion());
		
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
			return token[2] + "_" + token[3];
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
//			System.out.println(list.size() + "###");
			rd.generalTheRegion(tp._1(), list);
		}
		
	}
	
}
