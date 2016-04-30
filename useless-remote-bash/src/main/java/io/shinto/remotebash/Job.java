package io.shinto.remotebash;

import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

/**
 * Created by karel_alfonso on 17/04/2016.
 */
public class Job {
    private String id;
    private double cpus;
    private double mem;
    private String command;

    private JobState status;
    private int retries;

    private Job() {
        status = JobState.PENDING;
        id = UUID.randomUUID().toString();
        retries = 3;
    }

    public TaskInfo makeTask(FrameworkID frameworkId, SlaveID targetSlave) {
        TaskID taskID = TaskID.newBuilder()
                .setValue(id)
                .build();

        ExecutorID executorId = ExecutorID.newBuilder()
                .setValue(this.id)
                .build();

        CommandInfo commandInfo = CommandInfo.newBuilder()
                .setValue("java -jar useless-remote-bash-executor-1.0-SNAPSHOT.jar")
                .addUris(CommandInfo.URI.newBuilder()
                        .setValue("http://192.168.33.10:8000/useless-remote-bash-executor-1.0-SNAPSHOT.jar")
                        .build()
                )
                .build();

        ExecutorInfo executorInfo = ExecutorInfo.newBuilder()
                .setExecutorId(executorId)
                .setFrameworkId(frameworkId)
                .setCommand(commandInfo)
                .build();

        try {
            String taskData = command ;
            return TaskInfo.newBuilder()
                    .setName("task " + taskID.getValue())
                    .setTaskId(taskID)
                    .addResources(Resource.newBuilder()
                                    .setName("cpus")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(Value.Scalar.newBuilder().setValue(cpus))
                    )
                    .addResources(Resource.newBuilder()
                                    .setName("mem")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(Value.Scalar.newBuilder().setValue(mem))
                    )
                    .setData(ByteString.copyFrom(taskData.getBytes("UTF-8")))
                    .setExecutor(executorInfo)
                    .setSlaveId(targetSlave)
                    .build();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Job fromJson(JSONObject jsonObject) throws JSONException {
        Job job = new Job();
        job.cpus = jsonObject.getDouble("cpus");
        job.mem = jsonObject.getDouble("mem");
        job.command = jsonObject.getString("command");
        return job;
    }

    public void launch() {
        status = JobState.STAGING;
    }

    public void started() {
        status = JobState.RUNNING;
    }

    public void succeed() {
        status = JobState.SUCCESSFUL;
    }

    public void fail() {
        if (retries == 0) {
            status = JobState.FAILED;
        } else {
            retries--;
            status = JobState.PENDING;
        }
    }

    public String getId() {
        return id;
    }

    public double getCpus() {
        return cpus;
    }

    public double getMem() {
        return mem;
    }

    public JobState getStatus() {
        return status;
    }

    public String getCommand() {
        return command;
    }

    public boolean fitsIn(double offerCpus, double offerMem) {
        return cpus <= offerCpus && mem <= offerMem;
    }
}
