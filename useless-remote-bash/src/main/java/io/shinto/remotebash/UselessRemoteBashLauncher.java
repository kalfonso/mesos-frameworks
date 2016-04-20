package io.shinto.remotebash;

import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.RetryOneTime;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.zookeeper.KeeperException;
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
    public static void main(String[] args) throws Exception {
        CuratorFramework curator = startInHighAvailabilityMode(args[1]);
        List<Job> jobs = parseJobs();

        Protos.FrameworkInfo.Builder frameworkInfoBuilder = Protos.FrameworkInfo.newBuilder()
                .setUser("")
                .setName("Useless Remote Bash")
                .setFailoverTimeout(60 * 60 * 24 * 7);

        try {
            byte[] curatorData = curator.getData().forPath("/sampleframework/id");
            frameworkInfoBuilder.setId(
                    Protos.FrameworkID.newBuilder().setValue(new String(curatorData, "UTF-8"))
            );
        } catch (KeeperException.NoNodeException e) {
            // Don't set framework ID.
            // Log error
            e.printStackTrace();
        }

        Scheduler scheduler = new UselessRemoteBash(jobs, curator);

        SchedulerDriver schedulerDriver = new MesosSchedulerDriver(
                scheduler,
                frameworkInfoBuilder.build(),
                args[0] + "/mesos"
        );
        schedulerDriver.run();
    }

    private static CuratorFramework startInHighAvailabilityMode(String zkHostAndPort) throws Exception {
        CuratorFramework curator = CuratorFrameworkFactory.newClient(
                zkHostAndPort,
                new RetryOneTime(1000)
        );
        curator.start();

//        LeaderLatch leaderLatch = new LeaderLatch(curator, "/sampleframework/leader");
//        leaderLatch.start();
//        leaderLatch.await();

        return curator;
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
