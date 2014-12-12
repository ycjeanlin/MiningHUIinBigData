/*
 * The following steps are all executed on hadoop cluster
 * Compile: javac fetchFile.java -classpath hadoop-core-0.20.205.0.jar -d fs_class -Xlint
 * Create runable jar file:   jar -cvf ./fetchFile.jar -C fs_class/ .
 * Execution: hadoop jar fetchFile.jar idb.PMiningTWU.fetchFile /user/user09/output/part-00000
 */

package idb.PMiningTWU;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class uploadFile {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		//"/user/user09/Patterns/parallelcounting/part-r-*"
		Path outFile = new Path(args[1]);

		try {
			FileSystem fs = FileSystem.get(conf);
			InputStream in = new BufferedInputStream(new FileInputStream(
			        new File(args[0])));
			
			 if (fs.exists(outFile)){
				 fs.delete(outFile, true);
			 }

				System.out.println("Output File");
				FSDataOutputStream out = fs.create(outFile);
				
			//read the local file and copy the file to HDFS 
		    byte[] b = new byte[1024];
		    int numBytes = 0;
		    while ((numBytes = in.read(b)) > 0) {
		        out.write(b, 0, numBytes);
		    }

		    in.close();
		    out.close();
			fs.close();
				  
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
