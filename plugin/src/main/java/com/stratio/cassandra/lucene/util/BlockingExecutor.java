/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.cassandra.lucene.util;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is a specialized extension of the ThreadPoolExecutor class.
 *
 * Two functionalities had been added to this subclass. 1) The execute method of the ThreadPoolExecutor will block in
 * case the queue is full and only unblock when the queue is dequeued - that is a task that is currently in the queue is
 * removed and handled by the ThreadPoolExecutor. 2) Client code can await for the event of all tasks being run to
 * conclusion. Client code which actively chose to wait for this occurrence should call await on the instance of his
 * ThreadPoolExecutor. This differs from awaitTermination as it does not require any call to shutdown.
 *
 * This subclass of ThreadPoolExecutor also takes away the max threads capabilities of the ThreadPoolExecutor superclass
 * and internally sets the amount of maximum threads to be the size of the core threads. This is done since threads over
 * the core size and under the max are instantiated only once the queue is full, but the BlockingExecutor will block
 * once the queue is full.
 *
 * @author Yaneeve Shekel {@literal &} Amir Kirsh
 */
public class BlockingExecutor extends ThreadPoolExecutor {

    /**
     * Counts the number of current tasks in process.
     */
    private final AtomicInteger tasksInProcess = new AtomicInteger();

    /**
     * This is the Synchronizer instance that is used in order to notify all interested code of when all the tasks that
     * have been submitted to the execute() method have run to conclusion. This notification can occur a numerous amount
     * of times. It is all up to the client code. Whenever the ThreadPoolExecutor concludes to run all the tasks the
     * Synchronizer object will be notified and will in turn notify the code which is waiting on it.
     */
    private final Synchronizer synchronizer = new Synchronizer();

    /**
     * This constructor is used in order to maintain the first functionality specified above. It does so by using an
     * ArrayBlockingQueue and the BlockThenRunPolicy that is defined in this class. This constructor allows to give a
     * timeout for the wait on new task insertion and to react upon such a timeout if occurs.
     *
     * @param poolSize is the amount of threads that this pool may have alive at any given time
     * @param queueSize is the size of the queue. This number should be at least as the pool size to make sense
     * (otherwise there are unused threads), thus if the number sent is smaller, the poolSize is used for the size of
     * the queue. Recommended value is twice the poolSize.
     * @param keepAliveTime is the amount of time after which an inactive thread is terminated
     * @param keepAliveTimeUnit is the unit of time to use with the previous parameter
     * @param maxBlockingTime is the maximum time to wait on the queue of tasks before calling the BlockingTimeout
     * callback
     * @param maxBlockingTimeUnit is the unit of time to use with the previous parameter
     * @param blockingTimeCallback is the callback method to call when a timeout occurs while blocking on getting a new
     * task, the return value of this Callable is Boolean, indicating whether to keep blocking (true) or stop (false).
     * In case false is returned from the blockingTimeCallback, this executer will throw a RejectedExecutionException
     */
    public BlockingExecutor(int poolSize,
                            int queueSize,
                            long keepAliveTime,
                            TimeUnit keepAliveTimeUnit,
                            long maxBlockingTime,
                            TimeUnit maxBlockingTimeUnit,
                            Callable<Boolean> blockingTimeCallback) {

        super(poolSize, // Core size
              poolSize,      // Max size
              keepAliveTime, keepAliveTimeUnit,
              // not smaller than the poolSize (to avoid redundant threads)
              new ArrayBlockingQueue<>(Math.max(poolSize, queueSize), true),
              // When super invokes the reject method this class will ensure a blocking try
              new BlockThenRunPolicy(maxBlockingTime, maxBlockingTimeUnit, blockingTimeCallback));

        super.allowCoreThreadTimeOut(true); // Time out the core threads
    }

    /**
     * This constructor is used in order to maintain the first functionality specified above. It does so by using an
     * ArrayBlockingQueue and the BlockThenRunPolicy that is defined in this class. Using this constructor, waiting time
     * on new task insertion is unlimited.
     *
     * @param poolSize is the amount of threads that this pool may have alive at any given time.
     * @param queueSize is the size of the queue. This number should be at least as the pool size to make sense
     * (otherwise there are unused threads), thus if the number sent is smaller, the poolSize is used for the size of
     * the queue. Recommended value is twice the poolSize.
     * @param keepAliveTime is the amount of time after which an inactive thread is terminated.
     * @param unit is the unit of time to use with the previous parameter.
     */
    public BlockingExecutor(int poolSize, int queueSize, long keepAliveTime, TimeUnit unit) {

        super(poolSize,      // Core size
              poolSize,      // Max size
              keepAliveTime, unit,
              // not smaller than the poolSize (to avoid redundant threads)
              new ArrayBlockingQueue<>(Math.max(poolSize, queueSize), true),
              // When super invokes the reject method this class will ensure a blocking try.
              new BlockThenRunPolicy());

        super.allowCoreThreadTimeOut(true); // Time out the core threads.
    }

    /**
     * Before calling super's version of this method, the amount of tasks which are currently in process is first
     * incremented.
     *
     * @see ThreadPoolExecutor#execute(Runnable)
     */
    @Override
    public void execute(Runnable task) {
        // count a new task in process
        tasksInProcess.incrementAndGet();
        try {
            super.execute(task);
        } catch (RuntimeException | Error e) { // specifically handle RejectedExecutionException
            tasksInProcess.decrementAndGet();
            throw e;
        }
    }

    /**
     * After calling super's implementation of this method, the amount of tasks which are currently in process is
     * decremented. Finally, if the amount of tasks currently running is zero the synchronizer's signallAll() method is
     * invoked, thus anyone awaiting on this instance of ThreadPoolExecutor is released.
     *
     * @see ThreadPoolExecutor#afterExecute(Runnable, Throwable)
     */
    @Override
    protected void afterExecute(Runnable r, Throwable t) {

        super.afterExecute(r, t);

        // synchronizing on the pool (and actually all its threads)
        // the synchronization is needed to avoid more than one signal if two or more
        // threads decrement almost together and come to the if with 0 tasks together
        synchronized (this) {
            tasksInProcess.decrementAndGet();
            if (tasksInProcess.intValue() == 0) {
                synchronizer.signalAll();
            }
        }
    }

    /**
     * Internally calls on super's setCorePoolSize and setMaximumPoolSize methods with the given method argument.
     *
     * @see ThreadPoolExecutor#setCorePoolSize(int)
     */
    @Override
    public void setCorePoolSize(int corePoolSize) {
        super.setCorePoolSize(corePoolSize);
        super.setMaximumPoolSize(corePoolSize);
    }

    /**
     * Does Nothing!
     *
     * @throws UnsupportedOperationException in any event
     * @see ThreadPoolExecutor#setMaximumPoolSize(int)
     */
    @Override
    public void setMaximumPoolSize(int maximumPoolSize) {
        throw new UnsupportedOperationException("setMaximumPoolSize is not supported.");
    }

    /**
     * Does Nothing! MUST NOT CHANGE OUR BUILT IN RejectedExecutionHandler
     *
     * @throws UnsupportedOperationException in any event
     * @see ThreadPoolExecutor#setRejectedExecutionHandler(RejectedExecutionHandler)
     */
    @Override
    public void setRejectedExecutionHandler(RejectedExecutionHandler handler) {
        throw new UnsupportedOperationException("setRejectedExecutionHandler is not allowed on this class.");
    }

    /**
     * A blocking wait for this ThreadPool to be in idle state, which means that there are no more tasks in the Queue or
     * currently executed by one of the threads. BE AWARE that this method may get out from blocking state when a task
     * is currently sent to the ThreadPool not from this thread context. Thus it is not safe to call this method in case
     * there are several threads feeding the TreadPool with tasks (calling execute). The safe way to call this method is
     * from the thread that is calling execute and when there is only one such thread. Note that this method differs
     * from awaitTemination, as it can be called without shutting down the ThreadPoolExecuter.
     *
     * @throws InterruptedException when the internal condition throws it.
     */
    public void await() throws InterruptedException {
        synchronizer.await();
    }

    /**
     * A blocking wait for this ThreadPool to be in idle state or a certain timeout to elapse. Works the same as the
     * await() method, except for adding the timeout condition.
     *
     * @param timeout The timeout.
     * @param timeUnit The time unit.
     * @return false if the timeout elapsed, true if the synch event we are waiting for had happened.
     * @throws InterruptedException when the internal condition throws it.
     */
    public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return synchronizer.await(timeout, timeUnit);
    }

    // ====================================================================
    // start of inner private class Synchronizer
    // ====================================================================

    /**
     * This inner class serves to notify all interested parties that the ThreadPoolExecutor has finished running all the
     * tasks given to its execute method.
     */
    private class Synchronizer {

        private final Lock lock = new ReentrantLock();
        private final Condition done = lock.newCondition();
        private boolean isDone = false;

        /**
         * This PRIVATE method allows the ThreadPoolExecutor to notify all interested parties that all tasks given to
         * the execute method have run to conclusion.
         */
        private void signalAll() {

            lock.lock(); // MUST lock!
            try {
                isDone = true; // To help the await method ascertain that it has not waken up
                // 'spuriously'
                done.signalAll();
            } finally {
                lock.unlock(); // Make sure to unlock even in case of an exception
            }
        }

        /**
         * This is the inner implementation for supporting the BlockingExecutor.await().
         *
         * @throws InterruptedException when the internal condition throws it.
         * @see BlockingExecutor#await() for details.
         */
        public void await() throws InterruptedException {

            lock.lock(); // MUST lock!
            try {
                while (!isDone) { // Ascertain that this is not a 'spurious wake-up'
                    done.await();
                }
            } finally {
                isDone = false; // for next time
                lock.unlock(); // Make sure to unlock even in case of an exception
            }
        }

        /**
         * Inner implementation for supporting the BlockingExecutor.await(timeout, timeUnit).
         *
         * @param timeout The await time.
         * @param timeUnit The await {@link TimeUnit}.
         * @throws InterruptedException when the internal condition throws it.
         * @see BlockingExecutor#await(long, TimeUnit) for details.
         */
        public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException {

            boolean awaitResult = false;
            lock.lock(); // MUST lock!
            boolean localIsDone;
            try {
                awaitResult = done.await(timeout, timeUnit);
            } finally {
                localIsDone = isDone;
                isDone = false; // for next time
                lock.unlock(); // Make sure to unlock even in case of an exception
            }
            // make sure we return true only if done!
            return awaitResult && localIsDone;
        }
    }

    /**
     * This Policy class enforces the blocking feature of the BlockingExecutor. It does so by invoking the
     * BlockingQueue's put method (instead of the offer method that is used by the standard implementation of the
     * ThreadPoolExecutor - see the opened Java 6 source code).
     */
    private static class BlockThenRunPolicy implements RejectedExecutionHandler {

        private long maxBlockingTime;
        private TimeUnit maxBlockingTimeUnit;
        private Callable<Boolean> blockingTimeCallback;

        BlockThenRunPolicy(long maxBlockingTime, TimeUnit maxBlockingTimeUnit, Callable<Boolean> blockingTimeCallback) {
            this.maxBlockingTime = maxBlockingTime;
            this.maxBlockingTimeUnit = maxBlockingTimeUnit;
            this.blockingTimeCallback = blockingTimeCallback;
        }

        public BlockThenRunPolicy() {
            // just keep the maxBlocking gang all null / 0
        }

        /**
         * When this method is invoked by the ThreadPoolExecutor's reject method it simply asks for the Executor's Queue
         * and calls on its put method which will Block (at least for the ArrayBlockingQueue).
         *
         * @see RejectedExecutionHandler#rejectedExecution(Runnable, ThreadPoolExecutor)
         */
        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {

            BlockingQueue<Runnable> workQueue = executor.getQueue();
            boolean taskSent = false;

            while (!taskSent) {

                if (executor.isShutdown()) {
                    throw new RejectedExecutionException(
                            "ThreadPoolExecutor has shutdown while attempting to offer a new task.");
                }

                try {
                    // check whether to offer (blocking) with a timeout or without
                    if (blockingTimeCallback != null) {
                        // put on the queue and block if no room is available, with a timeout
                        // the result of the call to offer says whether the task was accepted or not
                        if (workQueue.offer(task, maxBlockingTime, maxBlockingTimeUnit)) {
                            // task accepted
                            taskSent = true;
                        } else {
                            // task was not accepted - call the Callback
                            Boolean result;
                            try {
                                result = blockingTimeCallback.call();
                            } catch (Exception e) {
                                // we got an exception from the Callback, wrap it and throw
                                throw new RejectedExecutionException(e);
                            }
                            // if result if false we need to throw an exception
                            // otherwise, just continue with the loop
                            if (!result) {
                                throw new RejectedExecutionException("User decided to stop waiting for task insertion");
                            }
                        }

                    }
                    // no timeout
                    else {
                        // just send the task (blocking, if the queue is full)
                        workQueue.put(task);
                        // task accepted
                        taskSent = true;
                    }
                } catch (InterruptedException e) {
                    // someone woke us up and we need to go back to the offer/put call...
                }
            } // end of while for InterruptedException
        }

    }

}