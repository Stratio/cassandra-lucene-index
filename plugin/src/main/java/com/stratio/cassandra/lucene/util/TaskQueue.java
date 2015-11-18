/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.util;

import com.stratio.cassandra.lucene.IndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A queue that executes each submitted task using one of possibly several pooled threads. Tasks can be submitted with
 * an identifier, ensuring that all tasks with same identifier will be executed orderly in the same thread. Each thread
 * has its own task queue.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TaskQueue {

    private static final Logger logger = LoggerFactory.getLogger(TaskQueue.class);

    private final NotifyingBlockingThreadPoolExecutor[] pools;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Returns a new {@link TaskQueue}.
     *
     * @param numThreads The number of executor threads.
     * @param queuesSize The max number of tasks in each thread queue before blocking.
     */
    public TaskQueue(int numThreads, int queuesSize) {

        pools = new NotifyingBlockingThreadPoolExecutor[numThreads];
        for (int i = 0; i < numThreads; i++) {
            pools[i] = new NotifyingBlockingThreadPoolExecutor(1,
                                                               queuesSize,
                                                               Long.MAX_VALUE,
                                                               TimeUnit.DAYS,
                                                               0,
                                                               TimeUnit.NANOSECONDS,
                                                               null);
            pools[i].submit(new Runnable() {
                @Override
                public void run() {
                    logger.debug("Task queue starts");
                }
            });
        }
    }

    /**
     * Submits a non value-returning task for asynchronous execution.
     *
     * The specified identifier is used to choose the thread executor where the task will be queued. The selection and
     * load balancing is based in the {@link #hashCode()} of this identifier.
     *
     * @param id   The identifier of the task used to choose the thread executor where the task will be queued for
     *             asynchronous execution.
     * @param task A task to be queued for asynchronous execution.
     * @return A future for the submitted task.
     */
    public Future<?> submitAsynchronous(Object id, Runnable task) {
        lock.readLock().lock();
        try {
            int i = Math.abs(id.hashCode() % pools.length);
            return pools[i].submit(task);
        } catch (Exception e) {
            logger.error("Task queue submission failed", e);
            throw new IndexException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Submits a non value-returning task for synchronous execution. It waits for all synchronous tasks to be
     * completed.
     *
     * @param task A task to be executed synchronously.
     */
    public void submitSynchronous(Runnable task) {
        lock.writeLock().lock();
        try {
            await();
            task.run();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Await for task completion.
     */
    public void await() {
        lock.writeLock().lock();
        try {
            Future<?>[] futures = new Future<?>[pools.length];
            for (int i = 0; i < pools.length; i++) {
                Future<?> future = pools[i].submit(new Runnable() {
                    @Override
                    public void run() {
                    }
                });
                futures[i] = future;
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (InterruptedException e) {
            logger.error("Task queue await interrupted", e);
            throw new IndexException(e);
        } catch (ExecutionException e) {
            logger.error("Task queue await failed", e);
            throw new IndexException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Shutdowns this task.
     */
    public void shutdown() {
        lock.writeLock().lock();
        try {
            for (int i = 0; i < pools.length; i++) {
                pools[i].shutdown();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

}