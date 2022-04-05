package com.tnas.dzone.asyncexec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AsynchronousExecutor<T, E> {

	private static final Integer MINUTES_WAITING_THREADS = 1;
	
	private Integer numThreads;

	private ExecutorService executor;
	
	private List<E> outputList;
	
	public AsynchronousExecutor() {
		this.numThreads = Runtime.getRuntime().availableProcessors();
		this.executor = Executors.newFixedThreadPool(this.numThreads);
		this.outputList = new ArrayList<>();
	}
	
	public AsynchronousExecutor(int threads) {
		this.numThreads = threads;
		this.executor = Executors.newFixedThreadPool(this.numThreads);
		this.outputList = new ArrayList<>();
	}
	
	public void processStream(List<T> inputList, ElementConverter<T, E> converter) {
		this.outputList = inputList.stream().map(e -> converter.apply(e)).collect(Collectors.toList());
	}
	
	public void processParallelStream(List<T> inputList, ElementConverter<T, E> converter) {
		this.outputList = inputList.parallelStream().map(e -> converter.apply(e)).collect(Collectors.toList());
	}
	
	public void processPartition(List<T> inputList, ElementConverter<List<T>, List<E>> converter) {

		var partitioner = new CollectionPartitioner<T>(inputList, numThreads);

		IntStream.range(0, numThreads).forEach(t -> this.executor.execute(() -> {
			
			var thOutput = converter.apply(partitioner.get(t));
			
			if (Objects.nonNull(thOutput) && !thOutput.isEmpty()) {
				synchronized (this.outputList) {
					this.outputList.addAll(thOutput);
				}
			}
		}));
	}
	
	@SuppressWarnings("unchecked")
	public void processShallowArrayPartition(List<T> inputList, ElementConverter<T, E> converter) {
		
		var chunkSize = (inputList.size() % this.numThreads == 0) ? (inputList.size() / this.numThreads) : (inputList.size() / this.numThreads) + 1;
		Object[] outputArr = new Object[inputList.size()];
		
		IntStream.range(0, numThreads).forEach(t -> this.executor.execute(() -> {
			
			var fromIndex = t * chunkSize;
			var toIndex = Math.min(fromIndex + chunkSize, inputList.size());
			
			if (fromIndex > toIndex) {
				fromIndex = toIndex;
			}
			
			IntStream.range(fromIndex, toIndex).forEach(i -> outputArr[i] = converter.apply(inputList.get(i)));
		}));
		
		this.shutdown();
		this.outputList = (List<E>) Arrays.asList(outputArr);
	}
	
	public void processShallowPartition(List<T> inputList, ElementConverter<T, E> converter) {
		
		var chunkSize = (inputList.size() % this.numThreads == 0) ? (inputList.size() / this.numThreads) : (inputList.size() / this.numThreads) + 1;
		this.outputList = new ArrayList<>(Collections.nCopies(inputList.size(), null));
		
		IntStream.range(0, numThreads).forEach(t -> this.executor.execute(() -> {
			
			var fromIndex = t * chunkSize;
			var toIndex = Math.min(fromIndex + chunkSize, inputList.size());
			
			if (fromIndex > toIndex) {
				fromIndex = toIndex;
			}
			
			IntStream.range(fromIndex, toIndex).forEach(i -> this.outputList.set(i, converter.apply(inputList.get(i))));
		}));
	}
	
	public void shutdown() {
		
		this.executor.shutdown();
		
		try {
			this.executor.awaitTermination(MINUTES_WAITING_THREADS, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}

	public Integer getNumThreads() {
		return numThreads;
	}

	public void setNumThreads(Integer numThreads) {
		this.numThreads = numThreads;
	}

	public List<E> getOutput() {
		return this.outputList;
	}
}
