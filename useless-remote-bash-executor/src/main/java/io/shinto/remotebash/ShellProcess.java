package io.shinto.remotebash;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by karel_alfonso on 30/04/2016.
 */
public class ShellProcess {
    public static void main(String[] args) {
        List<String> commands = new ArrayList<String>();
        commands.add("bash");
        commands.add("-c");
        commands.add("sleep 1; echo hello");
        ProcessBuilder pb = new ProcessBuilder(commands);
        try {
            Process prs = pb.start();
            //System.in.read();
            String result = IOUtils.toString(prs.getInputStream());
            System.out.println(result);
            prs.destroy();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}

