package com.tnas.dzone.asyncexec;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jeasy.random.EasyRandom;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@TestMethodOrder(OrderAnnotation.class)
public class AsynchronousExecutorTest {

	private static final Integer NUM_ELEMENTS_TEST = 1000000;
	private static final int NUM_THREADS = 8;
	
	private static final int NUM_REPETITIONS = 5;
	private static final int START_REPETITION = 1;
	
	private static final int START_SET_THREADS = 1;
	private static final int NUM_SET_THREADS = 6;
	private static final int[] SET_THREADS = { 1, 2, 4, 6, 8, 10 };
	
	private static Long[] timeMemory;
	private static Long[][] threadsTimeMemory;
	private static int currentRepetion;
	private static int currentThreadSet;
	private static String currentTest;
			
	private EasyRandom generator;
	
	public AsynchronousExecutorTest() {
		this.generator = new EasyRandom();
	}

	private void memorizeTestInfo(int repetition, String testName) {
		
		currentRepetion = repetition;
		
		if (currentRepetion == START_REPETITION) {
			timeMemory = new Long[NUM_REPETITIONS];
			threadsTimeMemory = new Long[NUM_SET_THREADS][NUM_REPETITIONS];
			currentTest = testName;
		}
	}
	
	void computeCPUTime(Long time) {
		
			timeMemory[currentRepetion - 1] = time;
		
			if (currentRepetion == NUM_REPETITIONS) {
				var stats = Arrays.asList(timeMemory).stream().mapToLong(Long::longValue).summaryStatistics();
				System.out.println(String.format("%s: %f (ms)", currentTest, stats.getAverage()));
			}
	}
	
	void computeThreadCPUTime(Long time, int threadSet, int repetition) {
	
		if (threadSet == START_SET_THREADS && repetition == START_REPETITION) {
			threadsTimeMemory = new Long[NUM_SET_THREADS][NUM_REPETITIONS];		
		}
		
		threadsTimeMemory[threadSet - 1][repetition - 1] = time;
	
		if (currentThreadSet == NUM_SET_THREADS && repetition == NUM_REPETITIONS) {
			
			double[] averages = new double[NUM_THREADS];
			
			IntStream.range(0, NUM_SET_THREADS).forEach(i -> {
				var stats = Arrays.asList(threadsTimeMemory[i]).stream().mapToLong(Long::longValue).summaryStatistics();
				averages[i] = stats.getAverage();
				System.out.println(String.format("%s: %d threads - %f (ms) - SpeedUp: %f", 
						currentTest, SET_THREADS[i], averages[i], averages[0]/averages[i]));
			});
		}
	}
	
	@Order(1)
	@RepeatedTest(value = NUM_REPETITIONS)
	public void testStream(RepetitionInfo info) {
		
		this.memorizeTestInfo(info.getCurrentRepetition(), "testStream");
		var asyncExec = new AsynchronousExecutor<String, String>(NUM_THREADS); 
		var converter = new UpperCaseConverter();
		
		List<String> inputList = this.generator.objects(String.class, NUM_ELEMENTS_TEST).collect(Collectors.toList());
		
		Instant start = Instant.now();
		asyncExec.processStream(inputList, converter);
		Instant end = Instant.now();
		this.computeCPUTime(Duration.between(start, end).toMillis());
		
		Assertions.assertEquals(inputList.size(), asyncExec.getOutput().size());
	}
	
	@Order(2)
	@RepeatedTest(value = NUM_REPETITIONS)
	public void testParallelStream(RepetitionInfo info) {
		
		this.memorizeTestInfo(info.getCurrentRepetition(), "testParallelStream");
		var asyncExec = new AsynchronousExecutor<String, String>(NUM_THREADS); 
		var converter = new UpperCaseConverter();
		
		List<String> inputList = this.generator.objects(String.class, NUM_ELEMENTS_TEST).collect(Collectors.toList());
		
		Instant start = Instant.now();
		asyncExec.processParallelStream(inputList, converter);
		Instant end = Instant.now();
		this.computeCPUTime(Duration.between(start, end).toMillis());
		
		Assertions.assertEquals(inputList.size(), asyncExec.getOutput().size());
	}
	
	@Order(3)
	@RepeatedTest(value = NUM_REPETITIONS)
	public void testSublistPartition(RepetitionInfo info) {
		
		this.memorizeTestInfo(info.getCurrentRepetition(), "testSublistPartition");
		var asyncExec = new AsynchronousExecutor<String, String>(NUM_THREADS); 
		var converter = new CollectionUpperCaseConverter();
		
		List<String> inputList = this.generator.objects(String.class, NUM_ELEMENTS_TEST).collect(Collectors.toList());
		
		Instant start = Instant.now();
		asyncExec.processSublistPartition(inputList, converter);
		asyncExec.shutdown();
		Instant end = Instant.now();
		this.computeCPUTime(Duration.between(start, end).toMillis());
		
		Assertions.assertEquals(inputList.size(), asyncExec.getOutput().size());
	}
	
	@Order(4)
	@RepeatedTest(value = NUM_REPETITIONS)
	public void testShallowPartitionList(RepetitionInfo info) {
		
		this.memorizeTestInfo(info.getCurrentRepetition(), "testShallowPartitionList");
		var asyncExec = new AsynchronousExecutor<String, String>(NUM_THREADS); 
		var converter = new UpperCaseConverter();
		
		List<String> inputList = this.generator.objects(String.class, NUM_ELEMENTS_TEST).collect(Collectors.toList());
		var expectedList = inputList.stream().map(e -> converter.apply(e)).collect(Collectors.toList());
	
		Instant start = Instant.now();
		asyncExec.processShallowPartitionList(inputList, converter);
		asyncExec.shutdown();
		Instant end = Instant.now();
		this.computeCPUTime(Duration.between(start, end).toMillis());
		
		Assertions.assertEquals(inputList.size(), asyncExec.getOutput().size());
		Assertions.assertLinesMatch(expectedList, asyncExec.getOutput());
	}
	
	@Order(5)
	@RepeatedTest(value = NUM_REPETITIONS)
	public void testShallowPartitionArray(RepetitionInfo info) {
		
		this.memorizeTestInfo(info.getCurrentRepetition(), "testShallowPartitionArray");
		var asyncExec = new AsynchronousExecutor<String, String>(NUM_THREADS); 
		var converter = new UpperCaseConverter();
		
		List<String> inputList = this.generator.objects(String.class, NUM_ELEMENTS_TEST).collect(Collectors.toList());
		var expectedList = inputList.stream().map(e -> converter.apply(e)).collect(Collectors.toList());
	
		var start = Instant.now();
		asyncExec.processShallowPartitionArray(inputList, converter);
		asyncExec.shutdown();
		var end = Instant.now();
		this.computeCPUTime(Duration.between(start, end).toMillis());
				
		Assertions.assertEquals(inputList.size(), asyncExec.getOutput().size());
		Assertions.assertLinesMatch(expectedList, asyncExec.getOutput());
	}
	
	@Order(6)
	@ParameterizedTest
	@ValueSource(ints = {1, 2, 4, 6, 8, 10})
	public void speedUpShallowPartitionArray(int numThreads) {

		currentThreadSet = numThreads == 1 ? 1 : currentThreadSet + 1;
		currentTest = "speedUpShallowPartitionArray";
		
		IntStream.rangeClosed(1, NUM_REPETITIONS).forEach(repetition -> {
			
			var asyncExec = new AsynchronousExecutor<String, String>(numThreads); 
			var converter = new UpperCaseConverter();
			
			List<String> inputList = this.generator.objects(String.class, NUM_ELEMENTS_TEST).collect(Collectors.toList());
			
			var start = Instant.now();
			asyncExec.processShallowPartitionArray(inputList, converter);
			var end = Instant.now();
			this.computeThreadCPUTime(Duration.between(start, end).toMillis(), currentThreadSet, repetition);
			
			Assertions.assertEquals(inputList.size(), asyncExec.getOutput().size());
		}); 
		
	}
}
