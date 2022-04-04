package com.tnas.dzone.asyncexec;

import java.util.List;
import java.util.stream.Collectors;

public class CollectionUpperCaseConverter implements ElementConverter<List<String>, List<String>> {

	@Override
	public List<String> apply(List<String> param) {
		return param.stream().map(String::toUpperCase).collect(Collectors.toList());
	}


}
