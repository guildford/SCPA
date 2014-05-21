package nlsde.buaa.stationpassenger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import nlsde.buaa.base.util.CONSTANT;

public class CardReadIn {
	private final static String pathProperties = "conf"+File.separator+"data_path.properties";
	private static Properties p;
	
	public static void main(String[] args){
		Properties p=new Properties();
		try {
			p.load(new FileInputStream(pathProperties));
			System.out.println(p.getProperty("complete_card_data"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String cardDataPath = null;
	private String[] cardDate = null;

	private BufferedReader cardin = null;
	private File cardFile = null;
	private int dateIndex=0;
	
	public CardReadIn() {
		p=new Properties();
		try {
			FileInputStream in=new FileInputStream(pathProperties);
			p.load(in);
			System.out.println(p.getProperty("complete_card_data"));
			this.cardDataPath = p.getProperty("complete_card_data");
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(2);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(2);
		}
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
			return new CardBean(line,CONSTANT.CardType);
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