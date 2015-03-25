package bwatson.secondarysort;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

/**
 * Takes a line in the form "internalIp\texternalDomain" and emits it as a
 * Pair<internalIp,Pair<externalDomain,NULL>> so that a secondary sort can be
 * performed.
 * 
 * Inefficient and error-prone, but demonstrated in its simplest form to enable
 * the core concept of secondary sort to be easily understood.
 * 
 * @author Ben Watson
 */
public class HttpLogProcessor extends
		DoFn<String, Pair<String, Pair<String, String>>> {
	private static final String TAB_SEPARATOR = "\t";
	private static final String NULL_STRING = null;
	private String[] splitString;

	@Override
	public void process(String line,
			Emitter<Pair<String, Pair<String, String>>> emitter) {
		splitString = line.split(TAB_SEPARATOR);
		emitter.emit(Pair.of(splitString[0],
				Pair.of(splitString[1], NULL_STRING)));
	}
}