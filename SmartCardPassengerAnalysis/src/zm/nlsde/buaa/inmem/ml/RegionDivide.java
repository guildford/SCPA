package zm.nlsde.buaa.inmem.ml;

import org.apache.spark.api.java.JavaPairRDD;

import zm.nlsde.buaa.inmem.model.DataPool;

public class RegionDivide implements Runnable{
	
	private JavaPairRDD<String, String> POINT_COUNT; 
	
	@SuppressWarnings("unchecked")
	public RegionDivide() {
		this.POINT_COUNT = (JavaPairRDD<String, String>) DataPool.getInstance().get(this.getClass().getName(), "POINT_COUNT", true);
	}

	@Override
	public void run() {
		System.out.println(this.POINT_COUNT.take(10));
	}

}
