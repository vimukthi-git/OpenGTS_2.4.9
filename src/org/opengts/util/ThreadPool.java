// ----------------------------------------------------------------------------
// Copyright 2007-2013, GeoTelematic Solutions, Inc.
// All rights reserved
// ----------------------------------------------------------------------------
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// ----------------------------------------------------------------------------
// Description:
//  Thread pool manager
// ----------------------------------------------------------------------------
// Change History:
//  2006/03/26  Martin D. Flynn
//     -Initial release
//  2006/04/03  Martin D. Flynn
//     -Removed reference to JavaMail api imports
//  2006/06/30  Martin D. Flynn
//     -Repackaged
//  2006/11/28  Martin D. Flynn
//     -Added method "setMaxSize(size)"
//  2013/04/08  Martin D. Flynn
//     -Added global "StopThreads(...)" method to stop all active threads in all
//      ThreadPools.
// ----------------------------------------------------------------------------
package org.opengts.util;

import java.util.*;

/**
*** Thread pool manager
**/

public class ThreadPool
{
    
    // ------------------------------------------------------------------------

    private static final int    DFT_POOL_SIZE           = 20;
    private static final int    DFT_MAX_IDLE_AGE_SEC    = 0;
    private static final long   DFT_MAX_IDLE_AGE_MS     = (long)DFT_MAX_IDLE_AGE_SEC * 1000L;

    public  static final int    STOP_WAITING            = -1;
    public  static final int    STOP_NEVER              = 0;
    public  static final int    STOP_NOW                = 1;

    // ------------------------------------------------------------------------

    private static boolean                globalStopThreadsNow = false;
    private static Map<ThreadPool,String> threadPoolList       = new WeakHashMap<ThreadPool,String>();

    /**
    *** Adds a ThreadPool to a global list
    *** @param tp  A ThreadPool
    **/
    private static void _AddThreadPool(ThreadPool tp)
    {
        if (tp != null) {
            synchronized (ThreadPool.threadPoolList) {
                try {
                    if (!ThreadPool.threadPoolList.containsKey(tp)) {
                        ThreadPool.threadPoolList.put(tp,"");
                    }
                } catch (Throwable th) {
                    // not sure what could be thrown here for weak-references, but catch just in case
                    Print.logException("ThreadPool weak reference list", th);
                }
            }
        }
    }

    /**
    *** Tell all active threads to stop 
    *** @param stopNow  True to stop threads, even if jobs are still queued.  False
    ***                 to stop only after all jobs have been processed. (note that 
    ***                 jobs currently being processed will continue until they are
    ***                 done).
    **/
    public static void StopThreads(boolean stopNow)
    {
        synchronized (ThreadPool.threadPoolList) {
            if (stopNow) {
                ThreadPool.globalStopThreadsNow = true;
            }
            for (ThreadPool tp : ThreadPool.threadPoolList.keySet()) {
                tp.stopThreads(stopNow);
            }
        }
    }

    /**
    *** Tell all active threads to stop 
    **/
    public static int GetTotalThreadCount()
    {
        int count = 0;
        synchronized (ThreadPool.threadPoolList) {
            for (ThreadPool tp : ThreadPool.threadPoolList.keySet()) {
                count += tp.getSize();
            }
        }
        return count;
    }

    /**
    *** Tell all active threads to stop 
    **/
    public static int PrintThreadCount()
    {
        int count = 0;
        synchronized (ThreadPool.threadPoolList) {
            for (ThreadPool tp : ThreadPool.threadPoolList.keySet()) {
                String n = tp.getName();
                int    s = tp.getSize();
                Print.logInfo("ThreadPool '" + n + "' size=" + s);
                count += s;
            }
        }
        return count;
    }

    // ------------------------------------------------------------------------

    private ThreadGroup                 poolGroup       = null;
    private java.util.List<ThreadJob>   jobThreadPool   = null;
    private int                         maxPoolSize     = DFT_POOL_SIZE;
    private long                        maxIdleAgeMS    = DFT_MAX_IDLE_AGE_MS;
    private int                         threadId        = 1;
    private java.util.List<Runnable>    jobQueue        = null;
    private int                         waitingCount    = 0;
    private int                         stopThreads     = STOP_NEVER;

    /**
    *** Constuctor
    *** @param name The name of the thread pool
    **/
    public ThreadPool(String name)
    {
        this(name, DFT_POOL_SIZE, DFT_MAX_IDLE_AGE_SEC);
    }

    /**
    *** Constructor
    *** @param name The name of the thread pool
    *** @param maxPoolSize The maximum number of threads in the thread pool[CHECK]
    **/
    public ThreadPool(String name, int maxPoolSize)
    {
        this(name, maxPoolSize, DFT_MAX_IDLE_AGE_SEC);
    }
    
    /**
    *** Constructor
    *** @param name The name of the thread pool
    *** @param maxPoolSize   The maximum number of threads in the thread pool[CHECK]
    *** @param maxIdleSec    The maximum number of seconds a thread is allowed to remain
    ***                      idle before it self-terminates.
    **/
    public ThreadPool(String name, int maxPoolSize, int maxIdleSec)
    {
        super();
        this.poolGroup     = new ThreadGroup((name != null)? name : "ThreadPool");
        this.jobThreadPool = new Vector<ThreadJob>();
        this.jobQueue      = new Vector<Runnable>();
        this.setMaxSize(maxPoolSize);
        this.setMaxIdleSec(maxIdleSec);
        this.stopThreads   = ThreadPool.globalStopThreadsNow? STOP_NOW : STOP_NEVER;
        ThreadPool._AddThreadPool(this);
    }

    // ------------------------------------------------------------------------

    /**
    *** Gets the name of the thread pool
    *** @return The name of the thread pool
    **/
    public String getName()
    {
        return this.getThreadGroup().getName();
    }
    
    /**
    *** Returns the name of the thread pool
    *** @return The name of the thread pool
    **/
    public String toString()
    {
        return this.getName();
    }
    
    /**
    *** Returns true if this object is equal to <code>other</code>. This will
    *** only return true if they are the same object
    *** @param other The object to check equality with
    *** @return True if <code>other</code> is the same object
    **/
    public boolean equals(Object other)
    {
        return (this == other); // equals only if same object
    }
    
    // ------------------------------------------------------------------------
    
    /**
    *** Gets the thread group of the Threads in this pool
    *** @return The thread group of the Threads in this pool
    **/
    public ThreadGroup getThreadGroup()
    {
        return this.poolGroup;
    }

    // ------------------------------------------------------------------------

    /**
    *** Gets the current size of this thread pool
    *** @return The number of thread jobs in this thread pool
    **/
    public int getSize()
    {
        int size = 0;
        synchronized (this.jobThreadPool) {
            size = this.jobThreadPool.size();
        }
        return size;
    }

    /**
    *** Sets the maximum size of this thread pool
    *** @param maxSize The maximum size of the thread pool
    **/
    public void setMaxSize(int maxSize)
    {
        this.maxPoolSize = (maxSize > 0)? maxSize : DFT_POOL_SIZE;
    }

    /**
    *** Gets the maximum size of this thread pool
    *** @return The maximum size of the thread pool
    **/
    public int getMaxSize()
    {
        return this.maxPoolSize;
    }

    // ------------------------------------------------------------------------

    /**
    *** Sets the maximum number of seconds that a thread is allowed to remain idle
    *** before it self terminates.
    *** @param maxIdleSec The maximum number of idle seconds
    **/
    public void setMaxIdleSec(int maxIdleSec)
    {
        this.setMaxIdleMS((long)maxIdleSec * 1000L);
    }

    /**
    *** Sets the maximum number of milliseconds that a thread is allowed to remain idle
    *** before it self terminates.
    *** @param maxIdleMS The maximum number of idle milliseconds
    **/
    public void setMaxIdleMS(long maxIdleMS)
    {
        this.maxIdleAgeMS = (maxIdleMS > 0)? maxIdleMS : DFT_MAX_IDLE_AGE_MS;
    }

    /**
    *** Gets the maximum number of milliseconds that a thread is allowed to remain idle
    *** before it self terminates.
    *** @return The maximum idle milliseconds
    **/
    public long getMaxIdleMS()
    {
        return this.maxIdleAgeMS;
    }

    // ------------------------------------------------------------------------

    /**
    *** Adds a new job to the thread pool's queue
    *** @param job The job to add to the queue
    **/
    public void run(Runnable job)
    {
        if (job == null) {
            // quietly ignore null jobs
        } else
        if (this.stopThreads == STOP_NOW) {
            // quietly ignore job if this ThreadPool is in the process of stopping now.
        } else {
            synchronized (this.jobThreadPool) { // <-- modification of threadPool is likely
                synchronized (this.jobQueue) { // <-- modification of job queue mandatory
                    // It's possible that we may end up adding more threads than we need if this
                    // section executes multiple times before the newly added thread has a chance 
                    // to pull a job off the queue.
                    this.jobQueue.add(job);
                    if ((this.waitingCount == 0) && (this.jobThreadPool.size() < this.maxPoolSize)) {
                        ThreadJob tj = new ThreadJob(this, (this.getName() + "_" + (this.threadId++)));
                        this.jobThreadPool.add(tj);
                        Print.logDebug("New Thread: " + tj.getName() + " [" + this.getMaxSize() + "]");
                    }
                    this.jobQueue.notify(); // notify a waiting thread
                }
            }
        }
    }

    // ------------------------------------------------------------------------

    /**
    *** Stops all threads in this pool once queued jobs are complete
    **/
    public void stopThreads()
    {
        this.stopThreads(false); // stop when jobs are done
    }

    /**
    *** Stops all threads in this pool once queued jobs are complete
    *** @param stopNow  True to stop threads, even if jobs are still queued.  False
    ***                 to stop only after all jobs have been processed. (note that 
    ***                 jobs currently being processed will continue until they are
    ***                 done).
    **/
    public void stopThreads(boolean stopNow)
    {
        synchronized (this.jobQueue) {
            this.stopThreads = stopNow? STOP_NOW : STOP_WAITING;
            this.jobQueue.notifyAll();
        }
    }
    
    /**
    *** Removes the specified worker thread from the pool
    *** @param thread The thread to remove from the pool
    **/
    protected void _removeThreadJob(ThreadJob thread)
    {
        if (thread != null) {
            synchronized (this.jobThreadPool) {
                //Print.logDebug("Removing thread: " + thread.getName());
                this.jobThreadPool.remove(thread);
            }
        }
    }

    // ------------------------------------------------------------------------

    private static class ThreadJob
        extends Thread
    {
        private Runnable    job = null;
        private ThreadPool  threadPool = null;
        private long        creationTimeMS = 0L;
        private long        lastUsedTimeMS = 0L;

        public ThreadJob(ThreadPool pool, String name) {
            super(pool.getThreadGroup(), name);
            this.threadPool = pool;
            this.creationTimeMS = DateTime.getCurrentTimeMillis();
            this.lastUsedTimeMS = this.creationTimeMS;
            this.start(); // auto start thread
        }

        public void run() {

            /* loop forever (or until stopped) */
            while (true) {

                /* get next job */
                // 'this.job' is always null here
                boolean stop = false;
                synchronized (this.threadPool.jobQueue) {
                    //Print.logDebug("Thread checking for jobs: " + this.getName());
                    while (this.job == null) {
                        if (this.threadPool.stopThreads == STOP_NOW) {
                            // stop now, no more jobs
                            stop = true;
                            break;
                        } else
                        if (this.threadPool.jobQueue.size() > 0) {
                            // run next job
                            this.job = this.threadPool.jobQueue.remove(0); // Runnable
                        } else
                        if (this.threadPool.stopThreads == STOP_WAITING) {
                            // stop after all jobs have completed
                            stop = true;
                            break;
                        } else
                        if ((this.threadPool.maxIdleAgeMS > 0L) &&
                            ((DateTime.getCurrentTimeMillis() - this.lastUsedTimeMS) > this.threadPool.maxIdleAgeMS)) {
                            // stop due to excess idle time
                            stop = true;
                            break;
                        } else {
                            // wait for next job notification
                            int tmoMS = 20000;
                            // TODO: adjust 'tmpMS' to coincide with remaining 'maxIdleAgeMS'
                            this.threadPool.waitingCount++;
                            try { this.threadPool.jobQueue.wait(tmoMS); } catch (InterruptedException ie) {}
                            this.threadPool.waitingCount--;
                            // continue next loop
                        }
                    }
                }
                if (stop) { break; }

                /* run job */
                //Print.logDebug("Thread running: " + this.getName());
                this.job.run();
                this.job = null;
                this.lastUsedTimeMS = DateTime.getCurrentTimeMillis();

            } // while (true)

            /* remove thread from pool */
            this.threadPool._removeThreadJob(this);

        }

    } // class ThreadJob
    
    // ------------------------------------------------------------------------

    /**
    *** Main entry point for testing/debugging
    *** @param argv Comand-line arguments
    **/
    public static void main(String argv[])
    {
        RTConfig.setCommandLineArgs(argv);

        ThreadPool pool_1 = new ThreadPool("Test-1", 3);
        ThreadPool pool_2 = new ThreadPool("Test-2", 3);

        for (int i = 0; i < 15; i++) {
            final int n = i;
            Print.logInfo("Job " + i);
            Runnable job = new Runnable() {
                int num = n;
                public void run() {
                    Print.logInfo("Start Job: " + this.getName());
                    try { Thread.sleep(2000 + (num * 89)); } catch (Throwable t) {}
                    Print.logInfo("Stop  Job:                " + this.getName());
                }
                public String getName() {
                    return "[" + Thread.currentThread().getName() + "] " + num;
                }
            };
            if ((i & 1) == 0) {
                pool_1.run(job);
            } else {
                pool_2.run(job);
            }
            try { Thread.sleep(100); } catch (Throwable t) {}
        }

        Print.logInfo("Stop Threads");
        ThreadPool.StopThreads(true); // Stop now
        for (int i = 0; i < 20; i++) {
            Print.sysPrintln("---------------------------");
            int cnt = ThreadPool.PrintThreadCount();
            if (cnt <= 0) { break; }
            try { Thread.sleep(1000); } catch (Throwable t) {}
        }
        Print.sysPrintln("Total Thread Count: " + ThreadPool.GetTotalThreadCount());
        
    }
    
}
