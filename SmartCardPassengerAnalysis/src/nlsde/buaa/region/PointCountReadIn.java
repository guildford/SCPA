package nlsde.buaa.region;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

public class PointCountReadIn {
	private final static String DATA_PATH = "stationPassenger";

	private BufferedReader datain = null;
	private String date;

	public void setDate(String date) {
		this.date = date;
	}

	private File getDataFile() {
		return new File(PointCountReadIn.DATA_PATH + File.separator + this.date
				+ ".csv");
	}

	public PointCountBean getPointCountBean() {
		try {
			if (this.datain == null) {
				datain = new BufferedReader(new InputStreamReader(
						new FileInputStream(this.getDataFile()), "gbk"));
			}
			String line=null;
			line=datain.readLine();
			if (line==null){
				datain.close();
				datain = null;
				return null;
			}
			return new PointCountBean(line);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
