package io.shinto.remotebash;

import org.apache.curator.framework.CuratorFramework;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;


public class UselessRemoteBash implements Scheduler {

    private final Deque<Job> pendingJobs = new ConcurrentLinkedDeque<Job>();
    private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<String, Job>(16, 0.9f, 1);
    private final CuratorFramework curator;
    ReentrantLock lock = new ReentrantLock();

    public UselessRemoteBash(List<Job> jobList, CuratorFramework curator) {
        for (Job job : jobList) {
            pendingJobs.add(job);
            System.out.println("*** Adding job: " + job.getCommand());
        }
        this.curator = curator;
    }

    public void registered(SchedulerDriver driver, FrameworkID frameworkID, MasterInfo masterInfo) {
        System.out.println("Registered with framework: " + frameworkID);
        try {
            curator.create().creatingParentsIfNeeded().forPath("/sampleframework/id", frameworkID.getValue().getBytes("UTF-8"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
    }

    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
        Job job = getJobByTaskId(status);
        switch (status.getState()) {
            case TASK_RUNNING:
                job.started();
                break;
            case TASK_FINISHED:
                job.succeed();
                System.out.println("*** Removing job: " + job.getCommand());
                jobs.remove(status.getTaskId().getValue());
                break;
            case TASK_FAILED:
            case TASK_KILLED:
            case TASK_LOST:
            case TASK_ERROR:
                job.fail();
                if (job.getStatus().equals(JobState.PENDING)) {
                    try {
                        lock.lock();
                        pendingJobs.add(job);
                    } finally {
                        lock.unlock();
                    }
                }
                break;
            default:
                break;
        }
        System.out.println("Got status update " + status);
    }

    public void frameworkMessage(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, byte[] bytes) {

    }

    public void disconnected(SchedulerDriver schedulerDriver) {

    }

    public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveID) {

    }

    public void executorLost(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, int i) {

    }

    public void error(SchedulerDriver schedulerDriver, String s) {

    }

    public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
        for (Offer offer : offers) {
            if (pendingJobs.isEmpty()) {
                driver.declineOffer(offer.getId());
                break;
            }

            driver.launchTasks(
                    Collections.singletonList(offer.getId()),
                    doFirstFit(offer));
        }
    }

    public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerID) {

    }

    private List<TaskInfo> doFirstFit(Offer offer) {
        List<TaskInfo> toLaunch = new ArrayList<TaskInfo>();
        List<Job> launchedJobs = new ArrayList<Job>();
        double offerCpus = 0;
        double offerMem = 0;

        // Extract resource info from the offer
        for (Resource resource : offer.getResourcesList()) {
            if (resource.getName().equals("cpus")) {
                System.out.println("*** Offer CPU: " + resource.getScalar().getValue());
                offerCpus += resource.getScalar().getValue();
            } else if (resource.getName().equals("mem")) {
                System.out.println("*** Offer memory: " + resource.getScalar().getValue());
                offerMem += resource.getScalar().getValue();
            }
        }

        // Pack the pendingJobs that will fit the offer
        System.out.println("*** Packing jobs for available resources: " + offerCpus + " " + offerMem);
        lock.lock();
        try {
            for (Job job : pendingJobs) {
                if (job.fitsIn(offerCpus, offerMem)) {
                    System.out.println("*** Analysing job: " + job.getCommand());
                    toLaunch.add(job.makeTask(offer.getSlaveId()));
                    offerCpus -= job.getCpus();
                    offerMem -= job.getMem();

                    job.launch();
                    launchedJobs.add(job);
                    jobs.put(job.getId(), job);
                }
            }

            pendingJobs.removeAll(launchedJobs);
        } finally {
            lock.unlock();
        }

        System.out.println("*** About to launch: " + toLaunch.size() + " tasks");
        return toLaunch;
    }

    private Job getJobByTaskId(TaskStatus status) {
        return jobs.get(status.getTaskId().getValue());
    }
}
