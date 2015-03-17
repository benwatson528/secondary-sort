package com.bwatson.secondarysort;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestHttpLogProcessor {
	@Mock
	private Emitter<Pair<String, Pair<String, String>>> emitter;

	@Test
	public void testProcess() {
		HttpLogProcessor processor = new HttpLogProcessor();
		processor.process("10.1.1.1\tgoogle.com", emitter);

		Pair<String, String> innerPair = new Pair<String, String>("google.com",
				null);
		Pair<String, Pair<String, String>> outerPair = new Pair<String, Pair<String, String>>(
				"10.1.1.1", innerPair);
		verify(emitter).emit(outerPair);
		verifyNoMoreInteractions(emitter);
	}

}