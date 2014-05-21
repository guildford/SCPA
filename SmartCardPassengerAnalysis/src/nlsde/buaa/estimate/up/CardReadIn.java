package nlsde.buaa.estimate.up;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

public class CardReadIn {

	private String cardDataPath = null;
	private String[] cardDate = null;

	private BufferedReader cardin = null;
	private File cardFile = null;
	private int dateIndex=0;
	
	public CardReadIn() {
		this.cardDataPath = "G:\\cardData\\AFCcard";
//		this.cardDataPath = "F:\\study_data\\card\\AFCcard";
	}
	
	public void setCardDate(String[] date) {
		this.cardDate=date;
		this.dateIndex=0;
	}
	
	public CardBean getCardInfo() {
		try {
			if (cardin == null) {
				System.out.println("read file date:"+this.cardDate[this.dateIndex]);
				cardin = new BufferedReader(new InputStreamReader(
						new FileInputStream(this.getCardFile()), "gbk"));
			}
			String line;
			line = cardin.readLine();
			if (line == null) {
				cardin.close();
				cardin = null;
				if (this.dateIndex<this.cardDate.length-1){
					this.dateIndex++;
					return this.getCardInfo();
				}
				return null;
			}
			return new CardBean(line);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	

	private File getCardFile() {
		if (this.cardDate != null) {
			String cardfilename = this.cardDataPath + File.separator
					+ this.cardDate[this.dateIndex] + ".csv";
			this.cardFile = new File(cardfilename);
			return this.cardFile;
		}
		return null;
	}
}
