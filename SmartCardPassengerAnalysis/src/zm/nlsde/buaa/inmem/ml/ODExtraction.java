package zm.nlsde.buaa.inmem.ml;

import java.util.ArrayList;
import java.util.Random;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import zm.nlsde.buaa.inmem.model.DataPool;
import zm.nlsde.buaa.inmem.util.MapTool;

public class ODExtraction implements Runnable {

	private JavaRDD<String> SMARTCARD;
	private JavaRDD<String> STATION_INFO;
	private static ArrayList<String> bc_station_info;

	@SuppressWarnings("unchecked")
	public ODExtraction() {
		this.SMARTCARD = DataPool.getInstance().get(this.getClass().getName(), "SMARTCARD");
		this.STATION_INFO = DataPool.getInstance().get(this.getClass().getName(), "STATION_INFO");
		bc_station_info = (ArrayList<String>) DataPool.getInstance().broadcast(this.STATION_INFO.collect());
	}

	public Boolean dataReady() {
		return this.SMARTCARD != null && this.STATION_INFO != null;
	}

	@Override
	public void run() {
		// TODO OD at random
		System.out.println("Starting OD information Extraction...");
		JavaRDD<String> SMARTCARD_FULLOD = this.SMARTCARD.map(new ODAtRandom());
		
		if (DataPool.getInstance().put("SMARTCARD_FULLOD", SMARTCARD_FULLOD)) {
//			System.out.println("OD information Extraction Finished.");
		}
//		System.out.println(this.SMARTCARD.count());
	}

	@SuppressWarnings("serial")
	static class ODAtRandom extends Function<String, String> {
		
		Random rand = new Random(System.currentTimeMillis());
		String board, alight;
		
		@Override
		public String call(String ss) throws Exception {
			board = bc_station_info.get(rand.nextInt(bc_station_info.size()));
			alight = bc_station_info.get(rand.nextInt(bc_station_info.size()));
			
			String[] token = ss.split(",");
			String[] bsplits = board.split(",");
			String[] asplits = alight.split(",");
			double deltaseconds = MapTool.D_jw(Double.valueOf(bsplits[5]),
					Double.valueOf(bsplits[4]), Double.valueOf(asplits[5]),
					Double.valueOf(asplits[4])) / 30 * 3600;
			int day = Integer.valueOf(token[2].substring(6, 8));
			int hour = Integer.valueOf(token[2].substring(8, 10));
			int min = Integer.valueOf(token[2].substring(10, 12));
			int sec = Integer.valueOf(token[2].substring(12));
			hour += (int) deltaseconds / 3600;
			min += ((int) deltaseconds % 3600) / 60;
			sec += (int) deltaseconds % 60;
			if (sec > 59) {
				sec = 0;
				min++;
			}
			if (min > 59) {
				min = 0;
				hour++;
			}
			if (hour > 23) {
				hour = 0;
				day++;
			}
			String alightingtime = token[2].substring(0, 6)
					+ (day < 10 ? "0" + day : day)
					+ (hour < 10 ? "0" + hour : hour)
					+ (min < 10 ? "0" + min : min)
					+ (sec < 10 ? "0" + sec : sec);
			// flat-fare
			if (token[11].equalsIgnoreCase("0")) {

				return token[2] + "," + // boarding time
						"-1" + "," + // boarding stop
						bsplits[4] + "," + // boarding lon
						bsplits[5] + "," + // boarding lat
						alightingtime + "," + // alighting time
						"-1" + "," + // alighting stop
						asplits[4] + "," + // alighting lon
						asplits[5] + "," + // alighting lat
						token[6] + "," + // card No
						token[9] + "," + // line No
						token[10]; // vehicle No

			}
			// distance-based
			else {
				return token[2] + "," + // boarding time
						token[11] + "," + // boarding stop
						bsplits[4] + "," + // boarding lon
						bsplits[5] + "," + // boarding lat
						alightingtime + "," + // alighting time
						token[12] + "," + // alighting stop
						asplits[4] + "," + // alighting lon
						asplits[5] + "," + // alighting lat
						token[6] + "," + // card No
						token[9] + "," + // line No
						token[10]; // vehicle No
			}
		}

	}

}
