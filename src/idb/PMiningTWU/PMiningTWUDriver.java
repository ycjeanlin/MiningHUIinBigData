/*
 * Preparation:
 * 	[1] create "Input" directory and "Output" directory
 * 	[2] put the input file into "Input" directory
 * Exection: -i DB1.dat -o Output -h /user/user09/ -u 30
 */

package idb.PMiningTWU;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class PMiningTWUDriver {
	
	public static String InFile;
	public static String OutFile;
	public static int MinUtility;
	public static String HDFSPath;
	public static double minMinUtility;
	public static double maxMinUtility;
	public static double delta;


	public static void main(String[] args) throws Exception {
		long time = 0L;
		long numCandidate = 0L;
		List<Long> MRTime = new ArrayList<Long>();
		long startTime = 0;
        long endTime = 0;
        long totalTime =0;
        long totalUtility = 0;
        DecimalFormat df = new DecimalFormat("#0.00000");
        
		cmdParser(args);
		BufferedReader br = new BufferedReader(new FileReader("PMTWU_output/"+InFile));
		String line;
		while((line=br.readLine()) != null){
			String[] tran = line.split(":");
			totalUtility += Long.parseLong(tran[1]);
		}
		
		double minUtility = maxMinUtility;
		while(minUtility >= minMinUtility){
			System.out.println("-------minUtility = "+df.format(minUtility)+"-------");
			long utilityCount = (long)(	minUtility*(double)totalUtility);
		
	        startTime = System.currentTimeMillis();
	        
			PMiningTWU pmTWU = new PMiningTWU(utilityCount, HDFSPath);
			MRTime = pmTWU.runPMiningTWU(InFile);
			endTime = System.currentTimeMillis();
			totalTime = (endTime - startTime);
			
			numCandidate = pmTWU.exportResult("PMTWU_exp/HUI_list_t_"+df.format(minUtility)+"_"+OutFile, false);
			System.out.println("Total Time: "+totalTime);
			
			resultExport(minUtility, totalTime, MRTime, numCandidate);
			minUtility -= delta;
		}
		
		System.gc();
	}
	
	private static void resultExport(double minUtility, long time, List<Long> MRTime, long numCandidate) {
		BufferedWriter bw = null;
		
		try {
			bw = new BufferedWriter(new FileWriter("PMTWU_exp/time_"+OutFile,true));
			bw.write(minUtility+", "+time);
			bw.newLine();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			bw = new BufferedWriter(new FileWriter("PMTWU_exp/MRTime_"+OutFile,true));
			for(long t:MRTime){
				bw.write(minUtility+", "+t);
				bw.newLine();
			}
			bw.newLine();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			bw = new BufferedWriter(new FileWriter("PMTWU_exp/candidate_"+OutFile,true));
			bw.write(minUtility+", "+numCandidate);
			bw.newLine();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	
	public static boolean cmdParser(String[] args){
		Options options = new Options();
		options.addOption("i", true, "[input] input file or directory");
		options.addOption("o", true, "[output] output directory");
		options.addOption("h", true, "[HDFS Path]HDFS path");
		options.addOption("fu", true, "[From Minimum Utility] set the starting minimum utility");
		options.addOption("tu", true, "[To Minimum Utility] set the ending minimum utility");
		options.addOption("d", true, "[Delta] set the utility difference");
		
		CommandLineParser parser = new BasicParser();
		
		
		try {
			CommandLine cmd = parser.parse( options, args);
			if(	   cmd.hasOption("i") && cmd.hasOption("o") 
				&& cmd.hasOption("fu") && cmd.hasOption("tu")){
				InFile = cmd.getOptionValue("i");
				OutFile = cmd.getOptionValue("o");
				HDFSPath = cmd.getOptionValue("h");
				minMinUtility = Double.parseDouble(cmd.getOptionValue("fu"));
				maxMinUtility = Double.parseDouble(cmd.getOptionValue("tu"));
				delta = Double.parseDouble(cmd.getOptionValue("d"));
			}else{
				System.out.println("Commands Errors!");
				System.exit(0);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		System.out.println("-i "+InFile+" -o "+OutFile+" -h "+HDFSPath
				           +" -fu "+minMinUtility+" -tu "+maxMinUtility+" -d "+delta);

		return true;
	}

}
