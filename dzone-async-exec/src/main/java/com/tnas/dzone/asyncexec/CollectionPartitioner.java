package com.tnas.dzone.asyncexec;

import java.util.AbstractList;
import java.util.Collections;
import java.util.List;

public final class CollectionPartitioner<T> extends AbstractList<List<T>> {

	private final List<T> list;
	private final int chunkSize;
	
	public CollectionPartitioner(List<T> list, int numThreads) {
		this.list = list;
		this.chunkSize = (list.size() % numThreads == 0) ? (list.size() / numThreads) : (list.size() / numThreads) + 1;
	}
	
	@Override
	public synchronized List<T> get(int index) {
		var fromIndex = index * chunkSize;
		var toIndex = Math.min(fromIndex + chunkSize, list.size());
		
		if (fromIndex > toIndex) {
			return Collections.emptyList(); // Index out of allowed interval
	    }
		
		return this.list.subList(fromIndex, toIndex); 
	}

	@Override
	public int size() {
		return (int) Math.ceil((double) list.size() / (double) chunkSize);
	}
}
