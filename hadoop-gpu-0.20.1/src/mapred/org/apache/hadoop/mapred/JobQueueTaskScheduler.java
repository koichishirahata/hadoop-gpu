/***********************************************************************
 	hadoop-gpu
	Authors: Koichi Shirahata, Hitoshi Sato, Satoshi Matsuoka

This software is licensed under Apache License, Version 2.0 (the  "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-------------------------------------------------------------------------
File: JobQueueTaskScheduler.java
Version: 0.20.1
***********************************************************************/

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.lang.Math;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * A {@link TaskScheduler} that keeps jobs in a queue in priority order (FIFO
 * by default).
 */
class JobQueueTaskScheduler extends TaskScheduler {
  
  private static final int MIN_CLUSTER_SIZE_FOR_PADDING = 3;
  public static final Log LOG = LogFactory.getLog(JobQueueTaskScheduler.class);
  
  protected JobQueueJobInProgressListener jobQueueJobInProgressListener;
  protected EagerTaskInitializationListener eagerTaskInitializationListener;
  private float padFraction;
  
  public JobQueueTaskScheduler() {
    this.jobQueueJobInProgressListener = new JobQueueJobInProgressListener();
  }
  
  @Override
  public synchronized void start() throws IOException {
    super.start();
    taskTrackerManager.addJobInProgressListener(jobQueueJobInProgressListener);
    eagerTaskInitializationListener.setTaskTrackerManager(taskTrackerManager);
    eagerTaskInitializationListener.start();
    taskTrackerManager.addJobInProgressListener(
        eagerTaskInitializationListener);
  }
  
  @Override
  public synchronized void terminate() throws IOException {
    if (jobQueueJobInProgressListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          jobQueueJobInProgressListener);
    }
    if (eagerTaskInitializationListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          eagerTaskInitializationListener);
      eagerTaskInitializationListener.terminate();
    }
    super.terminate();
  }
  
  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    padFraction = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 
                                 0.01f);
    this.eagerTaskInitializationListener =
      new EagerTaskInitializationListener(conf);
  }

  @Override
  public synchronized List<Task> assignTasks(TaskTrackerStatus taskTracker)
      throws IOException {

    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    final int numTaskTrackers = clusterStatus.getTaskTrackers();
    final int clusterMapCapacity = clusterStatus.getMaxMapTasks();
    final int clusterReduceCapacity = clusterStatus.getMaxReduceTasks();

    Collection<JobInProgress> jobQueue =
      jobQueueJobInProgressListener.getJobQueue();

    //
    // Get map + reduce counts for the current tracker.
    //
    final int trackerMapCapacity = taskTracker.getMaxMapTasks();
    final int trackerCPUMapCapacity = taskTracker.getMaxCPUMapTasks();
    final int trackerGPUMapCapacity = taskTracker.getMaxGPUMapTasks();
    final int trackerReduceCapacity = taskTracker.getMaxReduceTasks();
    //final int trackerRunningMaps = taskTracker.countMapTasks();    
    final int trackerRunningCPUMaps = taskTracker.countCPUMapTasks();
    final int trackerRunningGPUMaps = taskTracker.countGPUMapTasks();
    //final int trackerRunningMaps = trackerRunningCPUMaps + trackerRunningGPUMaps;
    final int trackerRunningMaps = taskTracker.countMapTasks();
    final int trackerRunningReduces = taskTracker.countReduceTasks();

    LOG.info("XXXX trackerMapCapacity : " + trackerMapCapacity);
    LOG.info("XXXX trackerRunningCPUMaps : " + trackerRunningCPUMaps);
    LOG.info("XXXX trackerRunningGPUMaps : " + trackerRunningGPUMaps);
    LOG.info("XXXX trackerRunningMaps : " + trackerRunningMaps);
    
    // Assigned tasks
    List<Task> assignedTasks = new ArrayList<Task>();

    //
    // Compute (running + pending) map and reduce task numbers across pool
    //
    int remainingReduceLoad = 0;
    int remainingMapLoad = 0;
    int pendingMapLoad = 0;
    int finishedCPUMapTasks = 0;
    int finishedGPUMapTasks = 0;
    int cpuMapTaskMeanTime = 0;
    int gpuMapTaskMeanTime = 0;
    int totalMapTasks = 0;
    synchronized (jobQueue) {
      for (JobInProgress job : jobQueue) {
        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
          remainingMapLoad += (job.desiredMaps() - job.finishedMaps());
          pendingMapLoad += (remainingMapLoad - job.runningMaps());
          totalMapTasks += job.desiredMaps();
          finishedCPUMapTasks += job.finishedCPUMaps();
          finishedGPUMapTasks += job.finishedGPUMaps();
          cpuMapTaskMeanTime = job.getCPUMapTaskMeanTime();
          gpuMapTaskMeanTime = job.getGPUMapTaskMeanTime();
          LOG.info("job.desiredMaps : " + job.desiredMaps());
          LOG.info("job.finishedMaps : " + job.finishedMaps());
          LOG.info("job.maptaskmeantime : " + job.getMapTaskMeanTime());
          LOG.info("job.CPUmaptaskmeantime : " + job.getCPUMapTaskMeanTime());
          LOG.info("job.GPUmaptaskmeantime : " + job.getGPUMapTaskMeanTime());
          if (job.scheduleReduces()) {
            remainingReduceLoad += 
              (job.desiredReduces() - job.finishedReduces());
          }
        }
      }
    }
  	LOG.info("finishedCPUMaps : " + finishedCPUMapTasks);
  	LOG.info("finishedGPUMaps : " + finishedGPUMapTasks);
    LOG.info("reminingMapLoad : " + remainingMapLoad);
    LOG.info("pendingMapLoad : " + pendingMapLoad);
  	double accelarationFactor =
  		(cpuMapTaskMeanTime == 0 || gpuMapTaskMeanTime == 0) ? 0.0
  				: (double)cpuMapTaskMeanTime / (double)gpuMapTaskMeanTime;
  	LOG.info("accelarationfactor : " + accelarationFactor);
  	  	
  	//apply scheduling algorithm to MapTasks
	if (accelarationFactor != 0.0) {
		double fcpu = Math.ceil((double)pendingMapLoad / trackerCPUMapCapacity)
				* accelarationFactor;
		double fgpu = Math.ceil((double)pendingMapLoad / trackerGPUMapCapacity);
		double fmin = fgpu;
		int xmin = 0, ymin = pendingMapLoad;
		for (int x = 1; x < pendingMapLoad; x++) {
			int y = pendingMapLoad - x;
			double f = Math.max(Math.ceil(x / trackerCPUMapCapacity)
					* accelarationFactor, Math.ceil(y
					/ trackerGPUMapCapacity));
			if (f < fmin) {
				fmin = f;
				xmin = x;
				ymin = y;
			}
		}
		
		LOG.info("[fcpu_only, x, y, x/ncpu, y/ngpu] :" + " [" + fcpu
				* gpuMapTaskMeanTime + ", " + pendingMapLoad + ", " + 0
				+ ", " + pendingMapLoad / trackerCPUMapCapacity + ", " + 0
				+ "]");
		LOG.info("[fgpu_only, x, y, x/ncpu, y/ngpu] :" + " [" + fgpu
				* gpuMapTaskMeanTime + ", " + 0 + ", " + pendingMapLoad
				+ ", " + 0 + ", " + pendingMapLoad / trackerGPUMapCapacity
				+ "]");
		// fgreedy is used just for testing the scheduling effect
		double z = Math.ceil((double)pendingMapLoad
				/ (trackerCPUMapCapacity + trackerGPUMapCapacity));
		double fgreedy = Math.max(z * accelarationFactor, z);
		LOG.info("[fgreedy, x, y, x/ncpu, y/ngpu] :" + " [" + fgreedy
				* gpuMapTaskMeanTime + ", " + -1 + ", " + -1 + ", " + z
				+ ", " + z + "]");
		LOG.info("[f, x, y, x/ncpu, y/ngpu] :" + " [" + fmin
				* gpuMapTaskMeanTime + ", " + xmin + ", " + ymin + ", "
				+ xmin / trackerCPUMapCapacity + ", " + ymin
				/ trackerGPUMapCapacity + "]");
	}
  	
    // Compute the 'load factor' for maps and reduces
    double mapLoadFactor = 0.0;
    if (clusterMapCapacity > 0) {
      mapLoadFactor = (double)remainingMapLoad / clusterMapCapacity;
    }
    double reduceLoadFactor = 0.0;
    if (clusterReduceCapacity > 0) {
      reduceLoadFactor = (double)remainingReduceLoad / clusterReduceCapacity;
    }
        
    //
    // In the below steps, we allocate first map tasks (if appropriate),
    // and then reduce tasks if appropriate.  We go through all jobs
    // in order of job arrival; jobs only get serviced if their 
    // predecessors are serviced, too.
    //

    //
    // We assign tasks to the current taskTracker if the given machine 
    // has a workload that's less than the maximum load of that kind of
    // task.
    // However, if the cluster is close to getting loaded i.e. we don't
    // have enough _padding_ for speculative executions etc., we only 
    // schedule the "highest priority" task i.e. the task from the job 
    // with the highest priority.
    //
    
    //final int trackerCurrentMapCapacity = 
//      Math.min((int)Math.ceil(mapLoadFactor * trackerMapCapacity), 
//                              trackerMapCapacity);
    //int availableMapSlots = trackerCurrentMapCapacity - trackerRunningMaps;
    /** FIXTHIS **/
    int availableMapSlots = trackerMapCapacity - trackerRunningMaps;
    int availableCPUMapSlots = trackerCPUMapCapacity - trackerRunningCPUMaps;
    int availableGPUMapSlots = trackerGPUMapCapacity - trackerRunningGPUMaps;
    boolean exceededMapPadding = false;
    assert availableCPUMapSlots >= 0;
    //if (availableMapSlots > 0) {
    //  exceededMapPadding = 
//        exceededPadding(true, clusterStatus, trackerMapCapacity);
    //}
    
    int numLocalMaps = 0;
    int numNonLocalMaps = 0;
    LOG.info("XXXX availableMapSlots : " + availableMapSlots);
    LOG.info("XXXX availableCPUMapSlots : " + availableCPUMapSlots);
    LOG.info("XXXX availableGPUMapSlots : " + availableGPUMapSlots);

    //check if assign to CPU or not in aspect of scheduring algorithm
    if(pendingMapLoad >= accelarationFactor * trackerGPUMapCapacity) {
//    if(true) {
    	LOG.info("try to assign to CPU");
    	scheduleCPUMaps:
    	for (int i = 0; i < availableCPUMapSlots; ++i) {
    		synchronized (jobQueue) {
    			for (JobInProgress job : jobQueue) {
    				if (job.getStatus().getRunState() != JobStatus.RUNNING) {
    					continue;
    				}

    				Task t = null;

    				// Try to schedule a node-local or rack-local Map task
    				t = job.obtainNewLocalMapTask(taskTracker,
    						numTaskTrackers, taskTrackerManager
    						.getNumberOfUniqueHosts());
    				if (t != null) {
    					assignedTasks.add(t);
    					++numLocalMaps;
        		    	LOG.info("assign to CPU");
    					break;
    				}

    				t = job.obtainNewNonLocalMapTask(taskTracker,
    						numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts());
    				if (t != null) {
    					assignedTasks.add(t);
    					++numNonLocalMaps;
        		    	LOG.info("assign to CPU");
    					break scheduleCPUMaps;
    				}
    			}
    		}
    	}
    }
    else{
    	LOG.info("DO NOT try to assign to CPU");
    }
    
    scheduleGPUMaps:
    for (int i = 0; i < availableGPUMapSlots; ++i) {
    	synchronized(jobQueue) {
    		for (JobInProgress job : jobQueue) {
    			if (job.getStatus().getRunState() != JobStatus.RUNNING) {
    				continue;
    			}
    			
    			Task t = null;
    			
    			t = job.obtainNewLocalMapTask(taskTracker, numTaskTrackers,
    																		taskTrackerManager.getNumberOfUniqueHosts());
    			if (t != null) {
    				t.setRunOnGPU(true);
    				assignedTasks.add(t);
    				++numLocalMaps;
    		    	LOG.info("assign to GPU");
    				break;
    			}
    			
    			t = job.obtainNewNonLocalMapTask(taskTracker, numTaskTrackers, 
    																			taskTrackerManager.getNumberOfUniqueHosts());
    			if (t != null) {
    				t.setRunOnGPU(true);
    				assignedTasks.add(t);
    				++numNonLocalMaps;
    		    	LOG.info("assign to GPU");
    				break scheduleGPUMaps;
    			}
    		}
    	}
    }

    /*
    scheduleMaps:
    for (int i=0; i < availableMapSlots; ++i) {
      synchronized (jobQueue) {
        for (JobInProgress job : jobQueue) {
          if (job.getStatus().getRunState() != JobStatus.RUNNING) {
            continue;
          }

          Task t = null;
          
          // Try to schedule a node-local or rack-local Map task
          t = 
            job.obtainNewLocalMapTask(taskTracker, numTaskTrackers,
                                      taskTrackerManager.getNumberOfUniqueHosts());
          
          if (t != null) {
          	if (availableGPUMapSlots > 0) {
          		t.runOnGPU();
          		--availableGPUMapSlots;
          	}
            assignedTasks.add(t);
            ++numLocalMaps;
            
            // Don't assign map tasks to the hilt!
            // Leave some free slots in the cluster for future task-failures,
            // speculative tasks etc. beyond the highest priority job
            if (exceededMapPadding) {
              break scheduleMaps;
            }
           
            // Try all jobs again for the next Map task 
            break;
          }
          
          // Try to schedule a node-local or rack-local Map task
          t = 
            job.obtainNewNonLocalMapTask(taskTracker, numTaskTrackers,
                                   taskTrackerManager.getNumberOfUniqueHosts());
          
          if (t != null) {
            assignedTasks.add(t);
            ++numNonLocalMaps;
            
            // We assign at most 1 off-switch or speculative task
            // This is to prevent TaskTrackers from stealing local-tasks
            // from other TaskTrackers.
            break scheduleMaps;
          }
        }
      }
    }
    */
    //int assignedMaps = assignedTasks.size();

    //
    // Same thing, but for reduce tasks
    // However we _never_ assign more than 1 reduce task per heartbeat
    //
    final int trackerCurrentReduceCapacity = 
      Math.min((int)Math.ceil(reduceLoadFactor * trackerReduceCapacity), 
               trackerReduceCapacity);
    final int availableReduceSlots = 
      Math.min((trackerCurrentReduceCapacity - trackerRunningReduces), 1);
    boolean exceededReducePadding = false;
    if (availableReduceSlots > 0) {
      exceededReducePadding = exceededPadding(false, clusterStatus, 
                                              trackerReduceCapacity);
      synchronized (jobQueue) {
        for (JobInProgress job : jobQueue) {
          if (job.getStatus().getRunState() != JobStatus.RUNNING ||
              job.numReduceTasks == 0) {
            continue;
          }

          Task t = 
            job.obtainNewReduceTask(taskTracker, numTaskTrackers, 
                                    taskTrackerManager.getNumberOfUniqueHosts()
                                    );
          if (t != null) {
            assignedTasks.add(t);
            break;
          }
          
          // Don't assign reduce tasks to the hilt!
          // Leave some free slots in the cluster for future task-failures,
          // speculative tasks etc. beyond the highest priority job
          if (exceededReducePadding) {
            break;
          }
        }
      }
      
    }
    
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("Task assignments for " + taskTracker.getTrackerName() + " --> " +
//                "[" + mapLoadFactor + ", " + trackerMapCapacity + ", " + 
//                trackerCurrentMapCapacity + ", " + trackerRunningMaps + "] -> [" + 
//                (trackerCurrentMapCapacity - trackerRunningMaps) + ", " +
//                assignedMaps + " (" + numLocalMaps + ", " + numNonLocalMaps + 
//                ")] [" + reduceLoadFactor + ", " + trackerReduceCapacity + ", " + 
//                trackerCurrentReduceCapacity + "," + trackerRunningReduces + 
//                "] -> [" + (trackerCurrentReduceCapacity - trackerRunningReduces) + 
//                ", " + (assignedTasks.size()-assignedMaps) + "]");
//    }

    return assignedTasks;
  }

  private boolean exceededPadding(boolean isMapTask, 
                                  ClusterStatus clusterStatus, 
                                  int maxTaskTrackerSlots) { 
    int numTaskTrackers = clusterStatus.getTaskTrackers();
    int totalTasks = 
      (isMapTask) ? clusterStatus.getMapTasks() : 
        clusterStatus.getReduceTasks();
    int totalTaskCapacity = 
      isMapTask ? clusterStatus.getMaxMapTasks() : 
                  clusterStatus.getMaxReduceTasks();

    Collection<JobInProgress> jobQueue =
      jobQueueJobInProgressListener.getJobQueue();

    boolean exceededPadding = false;
    synchronized (jobQueue) {
      int totalNeededTasks = 0;
      for (JobInProgress job : jobQueue) {
        if (job.getStatus().getRunState() != JobStatus.RUNNING ||
            job.numReduceTasks == 0) {
          continue;
        }

        //
        // Beyond the highest-priority task, reserve a little 
        // room for failures and speculative executions; don't 
        // schedule tasks to the hilt.
        //
        totalNeededTasks += 
          isMapTask ? job.desiredMaps() : job.desiredReduces();
        int padding = 0;
        if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
          padding = 
            Math.min(maxTaskTrackerSlots,
                     (int) (totalNeededTasks * padFraction));
        }
        if (totalTasks + padding >= totalTaskCapacity) {
          exceededPadding = true;
          break;
        }
      }
    }

    return exceededPadding;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    return jobQueueJobInProgressListener.getJobQueue();
  }  
}
