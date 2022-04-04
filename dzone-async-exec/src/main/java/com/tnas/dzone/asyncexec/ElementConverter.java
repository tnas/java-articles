package com.tnas.dzone.asyncexec;

@FunctionalInterface
public interface ElementConverter<R, S> {
	S apply(R param);
}
