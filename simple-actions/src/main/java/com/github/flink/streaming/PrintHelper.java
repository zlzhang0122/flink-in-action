package com.github.flink.streaming;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.io.IoBuilder;

import java.io.PrintStream;

/**
 * add console log redirect
 *
 * @Author: zlzhang0122
 * @Date: 2021/7/7 2:24 下午
 */
public class PrintHelper {
    public static void helper(){
        PrintStream logger = IoBuilder.forLogger("System.out").setLevel(Level.DEBUG).buildPrintStream();
        System.setOut(logger);
    }
}
