package zm.nlsde.buaa.inmem.ml;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import yy.nlsde.buaa.stationpassenger.PointCountBean;
import zm.nlsde.buaa.inmem.model.DataPool;

public class PassCount implements Runnable {

	private JavaRDD<String> SMARTCARD_FULLOD;

	@SuppressWarnings("unchecked")
	public PassCount() {
		this.SMARTCARD_FULLOD = (JavaRDD<String>) DataPool.getInstance().get(this.getClass().getName(), "SMARTCARD_FULLOD");
	}

	@Override
	public void run() {
		JavaPairRDD<String, String> pointCountUp = this.SMARTCARD_FULLOD.keyBy(new KeyByLocationTimeUP());
		JavaPairRDD<String, String> pointCountDown = this.SMARTCARD_FULLOD.keyBy(new KeyByLocationTimeDown());
		
		Map<String, Object> upcount = pointCountUp.countByKey();
//		for (String key: upcount.keySet()) {
//			System.out.println(key + ": " + upcount.get(key));
//		}
		System.out.println("Passenger Count Finished with " + upcount.size() + " Boarding Keys.");
		
		Map<String, Object> downcount = pointCountDown.countByKey();
		System.out.println("Passenger Count Finished with " + downcount.size() + " Alighting Keys.");
		
		DataPool.getInstance().put("POINT_COUNT", pointCountUp.union(pointCountDown));
	}
	
	@SuppressWarnings("serial")
	static class KeyByLocationTimeUP extends Function<String, String> {

		@Override
		public String call(String ss) throws Exception {
			String[] token = ss.split(",");
			return token[2] + "," + token[3] + "," + token[0].substring(0, 12) + "," + PointCountBean.UP;
		}
		
	}
	
	@SuppressWarnings("serial")
	static class KeyByLocationTimeDown extends Function<String, String> {

		@Override
		public String call(String ss) throws Exception {
			String[] token = ss.split(",");
			return token[6] + "," + token[7] + "," + token[4].substring(0, 12) + "," + PointCountBean.DOWN;
		}
		
	}

}
