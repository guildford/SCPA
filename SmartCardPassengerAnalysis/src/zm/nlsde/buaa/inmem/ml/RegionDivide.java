package zm.nlsde.buaa.inmem.ml;

import org.apache.spark.api.java.JavaRDD;

import zm.nlsde.buaa.inmem.model.DataPool;

public class RegionDivide implements Runnable{
	
	private JavaRDD<String> POINT_COUNT; 
	
	public RegionDivide() {
		this.POINT_COUNT = DataPool.getInstance().get(this.getClass().getName(), "POINT_COUNT");
	}

	@Override
	public void run() {
		
	}

}
