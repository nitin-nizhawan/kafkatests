package com.github.nizhawan.nitin;

import org.slf4j.Logger;
import org.testcontainers.containers.output.OutputFrame;
import sun.rmi.runtime.Log;

import java.util.function.Consumer;

public class StdOutConsumer implements Consumer<OutputFrame> {
    @Override
    public void accept(OutputFrame outputFrame) {
        System.out.println(outputFrame.getUtf8String());
    }
}
