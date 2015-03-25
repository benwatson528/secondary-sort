package bwatson.secondarysort;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.hadoop.util.StringUtils;

/**
 * Takes the result of a secondary sort operation and counts the number of
 * unique domains that each internal IP has visited.
 * 
 * Inefficient and error-prone, but demonstrated in its simplest form to enable
 * the core concept of secondary sort to be easily understood.
 * 
 * @author Ben Watson
 */
public class CountUniqueDomains extends
		DoFn<Pair<String, Iterable<Pair<String, String>>>, String> {
	private String previousDomain;
	private String currentDomain;
	private long domainsVisited;

	@Override
	public void process(Pair<String, Iterable<Pair<String, String>>> input,
			Emitter<String> emitter) {
		this.domainsVisited = 0L;
		this.currentDomain = "";
		this.previousDomain = "";

		// Looping through each external domain. Domains are sorted so a count
		// can be maintained rather than a set
		for (Pair<String, String> pair : input.second()) {
			this.currentDomain = pair.first();
			if (!this.currentDomain.equals(this.previousDomain)) {
				this.domainsVisited++;
				this.previousDomain = this.currentDomain;
			}
		}
		emitter.emit(input.first() + StringUtils.COMMA_STR
				+ this.domainsVisited);
	}
}