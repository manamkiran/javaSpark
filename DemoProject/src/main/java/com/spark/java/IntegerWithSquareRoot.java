package com.spark.java;

public class IntegerWithSquareRoot {

	public final int number;

	public final double squareRoot;

	public IntegerWithSquareRoot(int number) {
		this.number = number;

		this.squareRoot = Math.sqrt(number);
	}

}
