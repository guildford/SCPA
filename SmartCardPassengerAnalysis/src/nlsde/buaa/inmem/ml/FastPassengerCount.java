package nlsde.buaa.inmem.ml;

import java.util.List;

import nlsde.buaa.inmem.boot.AppConf;
import nlsde.buaa.inmem.model.DataPool;
import nlsde.buaa.stationpassenger.PointCountBean;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class FastPassengerCount implements Runnable {

	private JavaRDD<String> SMARTCARD_FULLOD;

	@SuppressWarnings("unchecked")
	public FastPassengerCount() {
		this.SMARTCARD_FULLOD = (JavaRDD<String>) DataPool.getInstance().get(this.getClass().getName(), "SMARTCARD_FULLOD");
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		
		@SuppressWarnings("rawtypes")
		JavaPairRDD POINT_COUNT = this.SMARTCARD_FULLOD.keyBy(new KeyByLocationTimeUP())
//				                           .union(this.SMARTCARD_FULLOD.keyBy(new KeyByLocationTimeDown()))
				                           .groupByKey();
		
//		System.out.println(POINT_COUNT.count() + " $$$");
//		System.out.println(POINT_COUNT.first() + " $$$");
//		POINT_COUNT.foreach(new PrintAll());
		
		DataPool.getInstance().put("POINT_COUNT", POINT_COUNT);
		
	}
	
	@SuppressWarnings("serial")
	static class PrintAll extends VoidFunction<Tuple2<String, List<String>>> {

		@Override
		public void call(Tuple2<String, List<String>> tp)
				throws Exception {
			System.out.println(tp._1() + ": " + tp._2().size());
		}

	}
	
	@SuppressWarnings("serial")
	static class KeyByLocationTimeUP extends Function<String, String> {

		@Override
		public String call(String ss) throws Exception {
			String[] token = ss.split(",");
			return token[2] + "," + token[3] + "," + token[0].substring(0, AppConf.TIME_PRECISION) + "," + PointCountBean.UP;
		}
		
	}
	
	@SuppressWarnings("serial")
	static class KeyByLocationTimeDown extends Function<String, String> {

		@Override
		public String call(String ss) throws Exception {
			String[] token = ss.split(",");
			return token[6] + "," + token[7] + "," + token[4].substring(0, AppConf.TIME_PRECISION) + "," + PointCountBean.DOWN;
		}
		
	}
	
}
