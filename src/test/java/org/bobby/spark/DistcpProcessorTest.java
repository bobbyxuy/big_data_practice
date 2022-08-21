package org.bobby.spark;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class DistcpProcessorTest {
    @Test
    public void main() {
        String[] commands = Arrays.asList("distcp", "-i", "-m", "5", "input", "output").toArray(new String[0]);
        System.out.println("command " + Arrays.toString(commands));
        try {
            DistcpProcessor.main(commands);
        } catch (IOException e) {
            System.out.println("copy error " + e.getMessage());
        }
        // TODO: 缺少断言
    }
}