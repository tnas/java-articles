package com.tnas.dzone.asyncexec;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import org.jeasy.random.EasyRandom;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AsynchronousExecutorTest {

	private static final Integer NUM_ELEMENTS_TEST = 100000000;
	private EasyRandom generator;
	
	public AsynchronousExecutorTest() {
		this.generator = new EasyRandom();
	}
	
	@Test
	void stringsUppercase() {
		
		var asyncExec = new AsynchronousExecutor<String, String>(); 
		var converter = new CollectionUpperCaseConverter();
		
		List<String> inputList = this.generator.objects(String.class, NUM_ELEMENTS_TEST).collect(Collectors.toList());
		
		Instant start = Instant.now();
		asyncExec.processPartition(inputList, converter);
		asyncExec.shutdown();
		Instant end = Instant.now();
		
		System.out.println("stringsUppercase: " +  Duration.between(start, end).toMillis());
		
		Assertions.assertEquals(inputList.size(), asyncExec.getOutput().size());
	}
	
	@Test
	void shallowStringsUppercase() {
		
		var asyncExec = new AsynchronousExecutor<String, String>(); 
		var converter = new UpperCaseConverter();
		
		List<String> inputList = this.generator.objects(String.class, NUM_ELEMENTS_TEST).collect(Collectors.toList());
		var expectedList = inputList.stream().map(e -> converter.apply(e)).collect(Collectors.toList());
	
		Instant start = Instant.now();
		asyncExec.processShallowPartition(inputList, converter);
		Instant end = Instant.now();
		System.out.println("shallowStringsUppercase: " + Duration.between(start, end).toMillis());
		
		Assertions.assertEquals(inputList.size(), asyncExec.getOutput().size());
		Assertions.assertLinesMatch(expectedList, asyncExec.getOutput());
	}
	
	@ParameterizedTest
	@ValueSource(ints = {1, 2, 4, 6, 8, 10})
	void speedUpShallowStringsUpperCase(int numThreads) {
		
		var asyncExec = new AsynchronousExecutor<String, String>(numThreads); 
		var converter = new UpperCaseConverter();
		
		List<String> inputList = this.generator.objects(String.class, NUM_ELEMENTS_TEST).collect(Collectors.toList());
	
		var start = Instant.now();
		asyncExec.processShallowPartition(inputList, converter);
		var end = Instant.now();
		System.out.println(String.format("shallowStringsUppercase - numThreads: %d - CPU Time (ms): %d", numThreads, Duration.between(start, end).toMillis()));	
		
		Assertions.assertEquals(inputList.size(), asyncExec.getOutput().size());
	}
}
