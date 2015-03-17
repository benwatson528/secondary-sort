package com.bwatson.secondarysort;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Uses WordCount by jwills as a base for dependencies:
 * https://github.com/jwills/crunch-demo
 * 
 * Performs a secondary sort on HTTP log data to identify the number of unique
 * domains visited by each internal IP.
 * 
 * @author Ben Watson
 */
public class SecondarySortRunner extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Main(args, new Configuration());
	}

	public static int Main(String[] args, Configuration conf) throws Exception {
		return ToolRunner.run(conf, new SecondarySortRunner(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("crunch.planner.dotfile.outputdir", "/tmp/crunch-demo/dot/");

		if (!validArgs(args)) {
			return 1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		Pipeline pipeline = new MRPipeline(SecondarySortRunner.class, getConf());

		PCollection<String> lines = pipeline.readTextFile(inputPath);

		// Parse the log data into the correct format to be accepted by
		// secondary sort
		PTable<String, Pair<String, String>> parsedLogTable = lines.parallelDo(
				new HttpLogProcessor(),
				Writables.tableOf(
						Writables.strings(),
						Writables.pairs(Writables.strings(),
								Writables.strings())));

		// Performs the secondary sort
		PCollection<String> output = SecondarySort.sortAndApply(parsedLogTable,
				new CountUniqueDomains(), Writables.strings());

		pipeline.writeTextFile(output, outputPath);
		PipelineResult result = pipeline.done();
		return result.succeeded() ? 0 : 1;
	}

	private boolean validArgs(String[] args) {
		if (args.length != 2) {
			System.err
					.println("Usage: hadoop jar crunch-toolkit-1.0-SNAPSHOT-job.jar"
							+ " [generic options] input output");
			System.err.println();
			GenericOptionsParser.printGenericCommandUsage(System.err);
			return false;
		}
		return true;
	}
}
