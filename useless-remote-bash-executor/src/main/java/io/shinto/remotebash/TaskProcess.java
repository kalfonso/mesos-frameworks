package io.shinto.remotebash;


import org.apache.mesos.Protos;

public class TaskProcess implements Runnable {

    private Protos.TaskInfo taskInfo;
    private final Process process;
    private final ShellCommandExecutor shellCommandExecutor;

    public TaskProcess(Protos.TaskInfo taskInfo, Process process, ShellCommandExecutor shellCommandExecutor) {
        this.taskInfo = taskInfo;
        this.process = process;
        this.shellCommandExecutor = shellCommandExecutor;
    }

    public void run() {
        int exitCode;

        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            exitCode = -99;
        }

        if (exitCode == 0) {
            shellCommandExecutor.statusUpdate(taskInfo.getTaskId(), Protos.TaskState.TASK_FINISHED);
        } else {
            shellCommandExecutor.statusUpdate(taskInfo.getTaskId(), Protos.TaskState.TASK_FAILED);
        }
    }
}
