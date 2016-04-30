package io.shinto.remotebash;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by karel_alfonso on 19/04/2016.
 */
public class ShellCommandExecutor implements Executor {
    private ExecutorDriver executorDriver;

    private ConcurrentMap<TaskID, Process> processes = new ConcurrentHashMap<TaskID, Process>(16, 0.9f, 1);


    public void registered(ExecutorDriver executorDriver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
        this.executorDriver = executorDriver;
    }

    public void reregistered(ExecutorDriver executorDriver, SlaveInfo slaveInfo) {

    }

    public void disconnected(ExecutorDriver executorDriver) {

    }

    public void launchTask(ExecutorDriver executorDriver, TaskInfo taskInfo) {
        Process taskProcess = startProcess(taskInfo);
        processes.put(taskInfo.getTaskId(), taskProcess);
        statusUpdate(taskInfo.getTaskId(), TaskState.TASK_RUNNING);

        Thread thread = new Thread(new TaskProcess(taskInfo, taskProcess, this));
        thread.setDaemon(true);
        thread.start();
    }

    private Process startProcess(TaskInfo taskInfo) {
        List<String> commands = getCommand(taskInfo);
        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        String mesos_directory = System.getenv("MESOS_DIRECTORY");
        File stdoutFile = new File(mesos_directory, "child_stdout");
        File stderrFile = new File(mesos_directory, "child_stderr");

        processBuilder.redirectOutput(Redirect.to(stdoutFile));
        processBuilder.redirectError(Redirect.to(stderrFile));
        try {
            return processBuilder.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getCommand(TaskInfo taskInfo) {
        List<String> commands = new ArrayList<String>();
        String command = taskInfo.getData().toStringUtf8();
        commands.add("/bin/sh");
        commands.add("-c");
        commands.add(command);
        return commands;
    }

    public void killTask(ExecutorDriver executorDriver, TaskID taskID) {
        Process process = processes.remove(taskID);
        if (process != null) {
            process.destroy();
            statusUpdate(taskID, TaskState.TASK_KILLED);
        }
    }

    public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {

    }

    public void shutdown(ExecutorDriver executorDriver) {

    }

    public void error(ExecutorDriver executorDriver, String s) {

    }

    protected void statusUpdate(TaskID taskId, TaskState taskState) {
        TaskStatus status = TaskStatus.newBuilder()
                .setTaskId(taskId)
                .setState(taskState)
                .build();
        executorDriver.sendStatusUpdate(status);
    }

    public static void main(String[] args) {
        Executor executor = new ShellCommandExecutor();
        ExecutorDriver driver = new MesosExecutorDriver(executor);
        driver.run();
    }
}
