package bwatson.secondarysort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

import bwatson.secondarysort.SecondarySortRunner;

public class TestSecondarySort {
	private static final String PART_PREFIX = "part-";
	Configuration conf;
	String input = Thread.currentThread().getContextClassLoader()
			.getResource("http-log.txt").getPath();
	String outputFolder = "/tmp/crunch-demo/output/";

	@Test
	public void testSecondarySort() throws Exception {
		String[] args = new String[] { this.input, this.outputFolder };
		int result = SecondarySortRunner.Main(args, this.conf);
		assertEquals(0, result);
		Map<String, Long> expectedResultsMap = new HashMap<String, Long>();
		expectedResultsMap.put("10.1.2.1", 2L);
		expectedResultsMap.put("10.1.3.25", 1L);
		expectedResultsMap.put("10.1.4.2", 2L);

		Map<String, Long> resultsMap = readResults(this.outputFolder);
		assertEquals(expectedResultsMap.keySet().size(), resultsMap.keySet()
				.size());
		for (String expectedKey : expectedResultsMap.keySet()) {
			assertTrue(resultsMap.containsKey(expectedKey));
			assertEquals(expectedResultsMap.get(expectedKey),
					resultsMap.get(expectedKey));
		}
	}

	private Map<String, Long> readResults(String outputFolder)
			throws IOException {
		Map<String, Long> resultsMap = new HashMap<String, Long>();
		File[] listOfOutputFiles = new File(outputFolder).listFiles();
		for (File outputFile : listOfOutputFiles) {
			if (outputFile.getName().contains(PART_PREFIX)) {
				BufferedReader br = new BufferedReader(new FileReader(
						outputFile));
				try {
					String line = br.readLine();
					while (line != null && line.contains(StringUtils.COMMA_STR)) {
						String[] splitLine = line.split(StringUtils.COMMA_STR);
						resultsMap.put(splitLine[0],
								Long.parseLong(splitLine[1]));
						line = br.readLine();
					}
				} finally {
					br.close();
				}
			}
		}
		return resultsMap;
	}

	@Before
	public void setup() {
		FileUtils.deleteQuietly(new File(outputFolder));
		this.conf = new Configuration();
		this.conf.set("mapreduce.framework.name", "local");
		this.conf.set("fs.defaultFS", "file:///");
	}
}
