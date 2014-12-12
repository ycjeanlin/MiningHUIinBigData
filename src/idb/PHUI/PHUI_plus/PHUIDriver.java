/*
 * Preparation:
 * 	[1] create "Input" directory and "Output" directory
 * 	[2] put the input file into "Input" directory
 * Exection: -i DB1.dat -o Output -h /user/user09/ -u 30
 */

package idb.PHUI.PHUI_plus;

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


public class PHUIDriver {
	
	public static String InFile;
	public static String OutFile;
	public static int MinUtility;
	public static String HDFSPath;
	public static double minMinUtility;
	public static double maxMinUtility;
	public static double delta;


	public static void main(String[] args) throws Exception {
		ArrayList<Long> time = new ArrayList<Long>();
		ArrayList<Long> numCandidate = new ArrayList<Long>();
		List<List<Long>> MRTime = new ArrayList<List<Long>>();
		long startTime = 0;
        long endTime = 0;
        long totalTime = 0;
        long totalUtility = 0;
        DecimalFormat df = new DecimalFormat("#0.00000");
        
		
		cmdParser(args);
		BufferedReader br = new BufferedReader(new FileReader("PHUI_output/"+InFile));
		String line;
		int i = 1;
		while((line=br.readLine()) != null){
			String[] tran = line.split(":");
			totalUtility += Long.parseLong(tran[1]);
			//System.out.println("line: "+i++);
			if(tran.length<3){
				System.out.println("Something Wrong at "+i);
				System.exit(1);
			}
		}
		
		double minUtility = maxMinUtility;
		while(minUtility >= minMinUtility){
			long utilityCount = (long)(	minUtility*(double)totalUtility);
			System.out.println("-------minUtility = "+df.format(minUtility)+"-------");
			startTime = System.currentTimeMillis();
			
			PHUI pmHUI = new PHUI(utilityCount, HDFSPath);
			MRTime.add(pmHUI.runPHUI(InFile));
			endTime = System.currentTimeMillis();
			totalTime = (endTime - startTime);
			
			numCandidate.add(pmHUI.exportResult("PHUI_exp/HUI_list_t_"+df.format(minUtility)+"_"+OutFile, false));
			System.out.println("Total Time: "+totalTime);
			
			time.add(totalTime);
			minUtility -= delta;
		}
		
		resultExport(time, MRTime, numCandidate);
		
		System.gc();
	}
	
	private static void resultExport(ArrayList<Long> time,
			List<List<Long>> MRTime, ArrayList<Long> numCandidate) {
		BufferedWriter bw = null;
		
		try {
			bw = new BufferedWriter(new FileWriter("PHUIPlus_exp/time_"+OutFile));
			double minUtility = maxMinUtility;
			int index = 0;
			while(minUtility >=minMinUtility){
				bw.write(minUtility+", "+time.get(index));
				bw.newLine();
				index++;
				minUtility -= delta;
			}
			
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			bw = new BufferedWriter(new FileWriter("PHUIPlus_exp/MRTime_"+OutFile));
			double minUtility = maxMinUtility;
			int index = 0;
			while(minUtility >=minMinUtility){
				for(long t:MRTime.get(index)){
					bw.write(minUtility+", "+t);
					bw.newLine();
				}
				index++;
				minUtility -= delta;
			}
			
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		

		try {
			bw = new BufferedWriter(new FileWriter("PHUIPlus_exp/candidate"+OutFile));
			double minUtility = maxMinUtility;
			int index = 0;
			while(minUtility >=minMinUtility){
				bw.write(minUtility+", "+numCandidate.get(index));
				bw.newLine();
				index++;
				minUtility -= delta;
			}
			
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
