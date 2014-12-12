package idb.PMiningTWU;

import idb.PMiningTWU.DaraStructure.Itemset;
import idb.PMiningTWU.DaraStructure.Pair;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PMiningTWU {
	
	private long minUtility;
	private String HDFSPath;
	private String OutputPath = "PMTWU_output/";
	private String InputPath = "PMTWU_input/";
	private ArrayList<Itemset> HTWUItemset = new ArrayList<Itemset>();
	private List<Long> recNumCandidate = new ArrayList<Long>();
	
	public PMiningTWU(long minUtility, String HDFSPath){
		this.minUtility = minUtility;
		this.HDFSPath = HDFSPath;
	}
	
	public List<Long> runPMiningTWU(String inFile){
		String inFileOfUC = "count.dat";
		String outPathOfUC ="UCOutput/";
		String outFileOfUC = "UtilityCountOutput.dat";
		String outPathOfPMTWU = "PMOutput";
		String inFileOfPMTWU = "DB";
		String outFileOfPMTWU = "Result";
		ArrayList<Pair> fList = new ArrayList<Pair>();
		List<Long> MRTime = new ArrayList<Long>();
		HashMap<String, Integer> hFlist = new HashMap<String, Integer>(); 
		int level = 1;
		long startTime = 0;
		long endTime = 0;
		long totalTime = 0;
		 	
		copyFromLocal(OutputPath+inFile, inFileOfUC);
		
		outputDirectoryCheck(HDFSPath+outPathOfUC);
		
		try {
			runUtilityCount(HDFSPath+inFileOfUC,  HDFSPath+outPathOfUC);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		copyToLocal( HDFSPath+outPathOfUC, InputPath+outFileOfUC);
		
		fList = genFList( InputPath+outFileOfUC);
		 
		for(int rank=0;rank<fList.size();rank++){
			//System.out.println("Item Id:"+fList.get(rank).getItemId()+";"+fList.get(rank).getUtility());
			hFlist.put(fList.get(rank).getItemId()+"", rank);
		}
		System.out.println(hFlist.get("2"));
		reorderTransaction(OutputPath+inFile, OutputPath+inFileOfPMTWU+"_"+level+".dat", hFlist);
		
		level--;
		do{
			level++;
			startTime = System.currentTimeMillis();
			copyFromLocal(OutputPath+inFileOfPMTWU+"_"+level+".dat", HDFSPath+inFileOfPMTWU);
			System.out.println("MiningTWU file upload");
			
			outputDirectoryCheck(HDFSPath+outPathOfPMTWU);
			
			try {
				runMiningTWU(HDFSPath+inFileOfPMTWU, HDFSPath+outPathOfPMTWU);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			copyToLocal( HDFSPath+outPathOfPMTWU, InputPath+outFileOfPMTWU+"_"+level+".dat");
			endTime = System.currentTimeMillis();
			totalTime = (endTime - startTime);
			MRTime.add(totalTime);
			System.out.println("MiningTWU file download");
			
		}while(parseOutput(InputPath+outFileOfPMTWU+"_"+level+".dat", OutputPath+inFileOfPMTWU+"_"+(level+1)+".dat"));
		
		System.out.println("Mission Complete");
		return MRTime;
	}
	  

	private void outputDirectoryCheck(String HDFSOutputPath) {
		Configuration conf = new Configuration();
		Path inPath = new Path(HDFSOutputPath);//TODO if there are multiple output files
		BufferedWriter bw = null;
		
		try {
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(inPath)){
				fs.delete(inPath, true);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private boolean parseOutput(String inFile, String outFile) {
		BufferedReader br = null;
		BufferedWriter bw = null;
		String line;//1 - 2 3 4 :utility or 1 - :utility
		boolean nextItr = false;
		long numCandidate = 0;
		
		try {
			br = new BufferedReader(new FileReader(inFile));
			bw = new BufferedWriter(new FileWriter(outFile));
			
			while((line = br.readLine()) != null){
				String[] partition1 = line.split("-");
				String[] partition2 = partition1[1].split(":");
				
				if(partition2[0].length() == 0){
					String[] Itemset = partition1[0].trim().split(" ");
					long utility = Long.parseLong(partition2[1].trim());
					Itemset TWUItemset = new Itemset();
					
					TWUItemset.setUtility(utility);
					
					for(String item:Itemset){
						if(item != null){
							TWUItemset.addItem(Integer.parseInt(item.trim()));
						}
					}
					
					HTWUItemset.add(TWUItemset);
				}else{
					nextItr = true;
					bw.write(line);
					bw.newLine();
					numCandidate++;
				}
			}
			recNumCandidate.add(numCandidate);
			br.close();
			bw.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return nextItr;
	}

	private void reorderTransaction(String inFile, String inFileOfPMTWU, final HashMap<String, Integer> hFlist) {
		BufferedReader br = null;
		BufferedWriter bw = null;
		String line;
		
		try {
			br = new BufferedReader(new FileReader(inFile));
			bw = new BufferedWriter(new FileWriter(inFileOfPMTWU));

			while((line = br.readLine()) != null){
				ArrayList<String> transaction = new ArrayList<String>();
				String[] pair = line.split(":");
				StringTokenizer token = new StringTokenizer(pair[0], " ");

				while(token.hasMoreElements()){
					String item = token.nextToken();
					if(hFlist.containsKey(item)){
						transaction.add(item);
					}
				}
				
				//continue when the transaction does not have anything
				if(transaction.size() == 0){
					continue;
				}
				
				Collections.sort(transaction,
			            new Comparator<String>() {
			                public int compare(String item1, String item2) {
			                    return hFlist.get(item1) < hFlist.get(item2) ? -1 : hFlist.get(item1) == hFlist.get(item2) ? 0 : 1;
			                }
			            });
				
				bw.write("-");
				for(String item:transaction){
					bw.write(item+" ");
				}
				bw.write(":"+pair[1]);
				bw.newLine();
			}
			
			bw.close();
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void copyFromLocal(String localSrcFile, String HDFSDestFile) {
		Configuration conf = new Configuration();
		//"/user/user09/Patterns/parallelcounting/part-r-*"
		Path outFile = new Path(HDFSDestFile);

		try {
			FileSystem fs = FileSystem.get(conf);
			InputStream in = new BufferedInputStream(new FileInputStream(
			        new File(localSrcFile)));
			
			 if (fs.exists(outFile)){
				 fs.delete(outFile, true);
			 }

			 System.out.println("Upload file to HDFS");
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

	private ArrayList<Pair> genFList(String outUtilityCount) {
		ArrayList<Pair> fList = new ArrayList<Pair>();
		String line;
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(outUtilityCount));
			int key;
			int value;
			
			while((line = br.readLine()) != null){
				String[] pair = line.split("\t");
				key = Integer.parseInt(pair[0]);
				value = Integer.parseInt(pair[1]);
				
				if(value >= minUtility){
					Pair HUI = new Pair();
					HUI.setItem(key);
					HUI.setUtility(value);
					fList.add(HUI);
				}
				
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Collections.sort(fList,
	            new Comparator<Pair>() {
	                public int compare(Pair o1, Pair o2) {
	                    return o1.getUtility() < o2.getUtility() ? -1 : o1.getUtility() == o2.getUtility() ? 0 : 1;
	                }
	            });
		
		return fList;
	}

	
	private void copyToLocal(String HDFSSrcPath, String localDestFile) {
		Configuration conf = new Configuration();
		//"/user/user09/Patterns/parallelcounting/part-r-*"
		Path inPath = new Path(HDFSSrcPath);//TODO if there are multiple output files
		BufferedWriter bw = null;
		
		try {
			FileSystem fs = FileSystem.get(conf);
			/*OutputStream out = new BufferedOutputStream(new FileOutputStream(
			        new File(localDestFile)));*/
			FileStatus[] status = fs.listStatus(inPath);
			bw = new BufferedWriter(new FileWriter(localDestFile));
			String line = null;
			
			for(int i=0;i<status.length;i++){
				
				//ingnore the files like _SUCCESS or _logs
				if(status[i].getPath().getName().startsWith("_")){
					continue;
				}
				
				
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				
				if(br.ready()){
					System.out.println("Get File");
					
					while((line = br.readLine()) != null){
						bw.write(line);
						bw.newLine();
					}
				}

			    br.close();
			}
			
			fs.delete(inPath, true);
			
			bw.close();
			fs.close();
				  
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void runMiningTWU(String inFilePath, String outFilePath) throws Exception {
	    JobConf conf = new JobConf(PMiningTWU.class);
	    conf.setJobName("MiningTWU");
	    conf.setNumReduceTasks(4);
	
	    conf.setLong("MIN_UTILITY", minUtility);
	    
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);
	
	    conf.setMapperClass(MiningTWUMapper.class);
	    conf.setReducerClass(MiningTWUReducer.class);
	
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	
	    FileInputFormat.setInputPaths(conf, new Path(inFilePath));
	    FileOutputFormat.setOutputPath(conf, new Path(outFilePath));
	
	    JobClient.runJob(conf);
	}
	  
	public void runUtilityCount(String inFile, String outFile) throws Exception {
		JobConf conf = new JobConf(PMiningTWU.class);
		conf.setJobName("UtilityCount");
	
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(LongWritable.class);
	
	    conf.setMapperClass(UtilityCountMapper.class);
	    conf.setCombinerClass(UtilityCountReducer.class);
	    conf.setReducerClass(UtilityCountReducer.class);
	
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	
	    FileInputFormat.setInputPaths(conf, new Path(inFile));
	    FileOutputFormat.setOutputPath(conf, new Path(outFile));
	    
	    JobClient.runJob(conf);
	}

	public Long exportResult(String outFile, boolean print) {
		BufferedWriter bw = null;
		long totalNumCandidate = 0;
		
		try {
			bw = new BufferedWriter(new FileWriter(outFile));
			
			for(Itemset TWU:HTWUItemset){
				if(print){
					System.out.println(TWU.toString()+" utility: "+TWU.getUtility());
				}
				bw.write(TWU.toString()+" utility: "+TWU.getUtility());
				bw.newLine();
			}
			
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(long num:recNumCandidate){
			totalNumCandidate += num;
		}
		
		return totalNumCandidate;
	}

}
