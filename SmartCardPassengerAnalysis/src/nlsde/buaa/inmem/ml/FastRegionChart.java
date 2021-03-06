package nlsde.buaa.inmem.ml;

import java.util.List;

import nlsde.buaa.inmem.model.DataPool;
import nlsde.buaa.region.PointCountBean;
import nlsde.buaa.region.RegionDivide;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class FastRegionChart implements Runnable{
	
	private JavaPairRDD<String, List<String>> POINT_COUNT;
	
	public static RegionDivide rd;
	
	@SuppressWarnings("unchecked")
	public FastRegionChart(RegionDivide rd) {
		this.POINT_COUNT = (JavaPairRDD<String, List<String>>) DataPool.getInstance().get(this.getClass().getName(), "POINT_COUNT", true);
		FastRegionChart.rd = rd;
	}

	@Override
	public void run() {
		this.POINT_COUNT
		.foreach(new GeneralCharts());
	}

	@SuppressWarnings("serial")
	static class GeneralCharts extends VoidFunction<Tuple2<String, List<String>>> {

		@Override
		public void call(Tuple2<String, List<String>> tp) throws Exception {
			rd.generalTheChart(new PointCountBean(tp._1() + "," + tp._2().size()));
		}
		
	}
	
}
