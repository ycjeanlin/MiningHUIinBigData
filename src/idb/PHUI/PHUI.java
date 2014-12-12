package idb.PHUI;

import idb.PHUI.DaraStructure.Itemset;
import idb.PHUI.DaraStructure.Pair;

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

import org.apache.hadoop.conf.Configuration;
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

public class PHUI {
	
	private long minUtility;
	private String HDFSPath;
	private String OutputPath = "PHUI_output/";
	private String InputPath = "PHUI_input/";
	private ArrayList<Itemset> HUIs = new ArrayList<Itemset>();
	private List<Long> recNumCandidate = new ArrayList<Long>();
	
	public PHUI(long minUtility, String HDFSPath){
		this.minUtility = minUtility;
		this.HDFSPath = HDFSPath;
	}
	
	public List<Long> runPHUI(String inFile){
		String inFileOfUC = "count.dat";
		String outPathOfUC ="UCOutput/";
		String outFileOfUC = "UtilityCountOutput.dat";
		String outPathOfPMTWU = "PMOutput";
		String inFileOfPMTWU = "DB";
		String outFileOfPMTWU = "Result";
		ArrayList<Pair> fList = new ArrayList<Pair>();
		HashMap<Integer, Integer> hFlist = new HashMap<Integer, Integer>();
		List<Long> MRTime = new ArrayList<Long>();
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
			hFlist.put(fList.get(rank).getItemId(), rank);
		}
		
		reorderTransaction(OutputPath+inFile, OutputPath+inFileOfPMTWU+"_"+level+".dat", hFlist);
		
		level--;
		do{//timing MapReduce time
			level++;
			startTime = System.currentTimeMillis();
			copyFromLocal(OutputPath+inFileOfPMTWU+"_"+level+".dat", HDFSPath+inFileOfPMTWU);
			System.out.println("PHUI file upload");
			
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
			System.out.println("PHUI file download");
			
		}while(parseOutput(InputPath+outFileOfPMTWU+"_"+level+".dat", OutputPath+inFileOfPMTWU+"_"+(level+1)+".dat"));
		
		
		System.out.println("Mission Complete");
		
		return MRTime;
	}
	  

	private void outputDirectoryCheck(String HDFSOutputPath) {
		Configuration conf = new Configuration();
		Path inPath = new Path(HDFSOutputPath);
		
		try {
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(inPath)){
				fs.delete(inPath, true);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	private boolean parseOutput(String inFile, String outFile) {
		BufferedReader br = null;
		BufferedWriter bw = null;
		boolean nextItr = false;
		String line;//1 - 2 3 4 :potential utility:utility set or 1 -:utility
		long numCandidate = 0;
		
		try {
			br = new BufferedReader(new FileReader(inFile));
			bw = new BufferedWriter(new FileWriter(outFile));
			
			while((line = br.readLine()) != null){
				String[] partition1 = line.split("-");
				String[] partition2 = partition1[1].split(":");
				
				if(partition2[0].length() == 0){
					String[] Itemset = partition1[0].trim().split(" ");
					int utility = Integer.parseInt(partition2[1].trim());
					Itemset HUI = new Itemset();
					
					HUI.setUtility(utility);
					
					for(String item:Itemset){
						if(item != null){
							HUI.addItem(Integer.parseInt(item.trim()));
						}
					}
					
					HUIs.add(HUI);
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

	private void reorderTransaction(String inFile, String inFileOfPMTWU, final HashMap<Integer, Integer> hFlist) {
		BufferedReader br = null;
		BufferedWriter bw = null;
		String line;
		
		try {
			br = new BufferedReader(new FileReader(inFile));
			bw = new BufferedWriter(new FileWriter(inFileOfPMTWU));

			while((line = br.readLine()) != null){
				String[] partition = line.split(":");//transaction:potential utility:utility set set Ex. 1 2 3 4:28:8 6 8 6
				String[] items = partition[0].split(" ");
				String[] utilitySet = partition[2].split(" ");
				List<Pair> transaction = new ArrayList<Pair>();
				
				// prune the item which utility is lower than minimum utility
				for(int i=0;i<items.length;i++){
					Pair newItem = new Pair();
					newItem.setItem(Integer.parseInt(items[i]));
					newItem.setUtility(Integer.parseInt(utilitySet[i]));
					
					if(hFlist.containsKey(newItem.getItemId())){
						transaction.add(newItem);
					}
				}
				
				//continue when the transaction does not have anything
				if(transaction.size() == 0){
					continue;
				}
				
				Collections.sort(transaction,
			            new Comparator<Pair>() {
			                public int compare(Pair item1, Pair item2) {
			                    return hFlist.get(item1.getItemId()) < hFlist.get(item2.getItemId()) ? -1 : hFlist.get(item1.getItemId()) == hFlist.get(item2.getItemId()) ? 0 : 1;
			                }
			            });
				
				bw.write("-");
				for(Pair item:transaction){
					bw.write(item.getItemId()+" ");
				}
				bw.write(":"+partition[1]+":");
				for(Pair item:transaction){
					bw.write(item.getUtility()+" ");
				}
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
		Path inPath = new Path(HDFSSrcPath);//if there are multiple output files
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
	    JobConf conf = new JobConf(PHUI.class);
	    conf.setJobName("MiningHUI");
	    conf.setNumReduceTasks(4);
	    
	    conf.setLong("MIN_UTILITY", minUtility);
	    
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);
	
	    conf.setMapperClass(PHUIMapper.class);
	    conf.setReducerClass(PHUIReducer.class);
	
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	
	    FileInputFormat.setInputPaths(conf, new Path(inFilePath));
	    FileOutputFormat.setOutputPath(conf, new Path(outFilePath));
	
	    JobClient.runJob(conf);
	}
	  
	public void runUtilityCount(String inFile, String outFile) throws Exception {
		JobConf conf = new JobConf(PHUI.class);
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

	public long exportResult(String outFile, boolean print) {
		BufferedWriter bw = null;
		long totalNumCandidate = 0;
		
		try {
			bw = new BufferedWriter(new FileWriter(outFile));
			
			for(Itemset HUI:HUIs){
				if(print){
					System.out.println(HUI.toString()+" utility: "+HUI.getUtility());
				}
				bw.write(HUI.toString()+" utility: "+HUI.getUtility());
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
