/*
 * The following steps are all executed on hadoop cluster
 * Compile: javac fetchFile.java -classpath hadoop-core-0.20.205.0.jar -d fs_class -Xlint
 * Create runable jar file:   jar -cvf ./fetchFile.jar -C fs_class/ .
 * Execution: hadoop jar fetchFile.jar idb.PMiningTWU.fetchFile /user/user09/output/part-00000
 */

package idb.PMiningTWU;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class fetchFile {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		//"/user/user09/Patterns/parallelcounting/part-r-*"
		Path inFile = new Path(args[0]);

		try {
			FileSystem fs = FileSystem.get(conf);
			OutputStream out = new BufferedOutputStream(new FileOutputStream(
			        new File("test.txt")));
			
			
			if (!fs.exists(inFile)){
				System.out.println("Input file not found");
			}else{
				System.out.println("Get File");
				FSDataInputStream in = fs.open(inFile);
				
				//read the file and copy the file from HDFS to local system
			    byte[] b = new byte[1024];
			    int numBytes = 0;
			    while ((numBytes = in.read(b)) > 0) {
			        out.write(b, 0, numBytes);
			    }

			    in.close();
			    out.close();
				fs.close();
			}
				  
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
