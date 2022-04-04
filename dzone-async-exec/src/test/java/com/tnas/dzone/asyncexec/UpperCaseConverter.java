package com.tnas.dzone.asyncexec;

public class UpperCaseConverter implements ElementConverter<String, String> {

	@Override
	public String apply(String param) {
		return param.toUpperCase();
	}

}
