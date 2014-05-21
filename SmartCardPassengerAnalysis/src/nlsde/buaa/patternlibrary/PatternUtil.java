package nlsde.buaa.patternlibrary;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class PatternUtil {
	private String patternFile="C:\\study\\gradu\\pattern\\pattern_30";
	private String npatternFile="C:\\study\\gradu\\pattern\\n_pattern_30";
	
	public static void main(String[] args){
		new PatternUtil().file2file();
		new PatternUtil().readfile(100);
	}
	
	public void readfile(int lineNum){
		File pfile=new File(this.npatternFile);
		try {
			BufferedReader pin = new BufferedReader(new InputStreamReader(
					new FileInputStream(pfile), "gbk"));
			String line;
			int num=0;
			while ((line=pin.readLine())!=null){
				num++;
				if (num>lineNum){
					pin.close();
					return;
				}
				System.out.println(line);
			}
			pin.close();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void file2file(){
		File pfile=new File(this.patternFile);
		File nfile=new File(this.npatternFile);
		if (!pfile.exists())
			return;
		try {
			BufferedReader pin = new BufferedReader(new InputStreamReader(
					new FileInputStream(pfile), "gbk"));
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream(nfile), "gbk"), true);
			String line;
			int num=0;
			while ((line=pin.readLine())!=null){
				num++;
				if (num%1000000==0)
					System.out.println(num+":"+System.currentTimeMillis());
				String[] data=line.split("\t");
				if (data[1].contains(",")){
					String[] sub=data[1].split(",");
					String sb="";
					for (int i=0;i<sub.length;i+=4){
						sb+="#"+sub[i+0]+","+sub[i+1]+","+sub[i+2]+","+sub[i+3];
					}
					pw.println(data[0]+"\t"+sb.substring(1));
				}else{
					System.err.println("no pattern: "+data[0]+"\t"+data[1] );
				}
			}
			pin.close();
			pw.close();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
