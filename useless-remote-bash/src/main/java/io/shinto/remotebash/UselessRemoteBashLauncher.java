package io.shinto.remotebash;

import org.apache.commons.io.IOUtils;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by karel_alfonso on 16/04/2016.
 */
public class UselessRemoteBashLauncher {
    public static void main(String[] args) throws IOException {
        List<Job> jobs = parseJobs();

        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setUser("")
                .setName("Useless Remote Bash")
                .build();

        Scheduler scheduler = new UselessRemoteBash(jobs);

        SchedulerDriver schedulerDriver = new MesosSchedulerDriver(
                scheduler,
                frameworkInfo,
                args[0] + "/mesos"
        );
        schedulerDriver.run();
    }

    private static List<Job> parseJobs() throws IOException {
        URL jsonJobsUrl = ClassLoader.getSystemResource("jobs.json");
        String jsonJobs = IOUtils.toString(jsonJobsUrl);
        JSONObject config = new JSONObject(jsonJobs);
        JSONArray jobsArray = config.getJSONArray("jobs");
        List<Job> jobs = new ArrayList<Job>();
        for (int i = 0; i < jobsArray.length(); i++) {
            jobs.add(Job.fromJson(jobsArray.getJSONObject(i)));
        }
        return jobs;
    }
}
