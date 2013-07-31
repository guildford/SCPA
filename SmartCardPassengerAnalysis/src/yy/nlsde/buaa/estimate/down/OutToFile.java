package yy.nlsde.buaa.estimate.down;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class OutToFile {

	public static <T> void outToFile(List<T> list, String outfile) {
		mkdir(outfile, false);
		try {
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream(outfile), "gbk"), true);
			for (T t : list) {
				pw.println(t.toString());
			}
			pw.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public static void mkdir(String filename, boolean ifdir) {
		File dir = new File(filename);
		if (ifdir) {
			if (!dir.exists()) {
				dir.mkdirs();
			}
		} else {
			mkdir(dir.getParentFile().getAbsolutePath(), true);
		}
	}

}
