package com.alibaba.alinkexample;

import org.apache.flink.table.functions.ScalarFunction;

public class Multiple extends ScalarFunction {
    public static double eval(double a, double b) {
        return a * b;
    }
}
