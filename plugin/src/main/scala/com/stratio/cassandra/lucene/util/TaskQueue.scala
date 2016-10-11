/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.util

import java.io.Closeable
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.util.JavaConversions.asJavaCallable
import com.stratio.cassandra.lucene.util.TaskQueue._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionException

/** A queue that executes each submitted task using one of possibly several pooled threads. Tasks can be submitted with
  * an identifier, ensuring that all tasks with same identifier will be executed orderly in the same thread. Each thread
  * has its own task queue.
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
sealed trait TaskQueue extends Closeable {

  /** Submits a non value-returning task for asynchronous execution.
    *
    * The specified identifier is used to choose the thread executor where the task will be queued. The selection and
    * load balancing is based in the hashcode of the supplied id.
    *
    * @param id   the identifier of the task used to choose the thread executor where the task will be queued for
    *             asynchronous execution
    * @param task the task to be queued for asynchronous execution
    */
  def submitAsynchronous[A](id: AnyRef, task:() => A): Unit

  /**
    * Submits a non value-returning task for synchronous execution. It waits for all synchronous tasks to be
    * completed.
    *
    * @param task a task to be executed synchronously
    * @return the result of the task
    */
  def submitSynchronous[A](task:() => A): A
}

/** Trivial [[TaskQueue]] not using parallel nor asynchronous processing */
private class TaskQueueSync extends TaskQueue {

  /** @inheritdoc*/
  override def submitAsynchronous[A](id: AnyRef, task:() => A): Unit = task.apply

  /** @inheritdoc*/
  override def submitSynchronous[A](task:() => A): A = task.apply

  /** @inheritdoc*/
  override def close() = {}

}

/** [[TaskQueue]] using parallel processing with thread pools.
  *
  * @param numThreads the number of executor threads
  * @param queuesSize the max number of tasks in each thread queue before blocking
  */
private class TaskQueueAsync(numThreads: Int, queuesSize: Int) extends TaskQueue {

  private val pools = (1 to numThreads).map(i => new BlockingExecutor(1, queuesSize, 1, TimeUnit.DAYS))
  private val lock = new ReentrantReadWriteLock(true)

  /** @inheritdoc*/
  override def submitAsynchronous[A](id: AnyRef, task:() => A): Unit = {
    lock.readLock.lock()
    try {
      val pool = pools(Math.abs(id.hashCode % numThreads)) // Choose pool
      pool.submit(task)
    } catch {
      case e: Exception =>
        logger.error("Task queue asynchronous submission failed", e)
        throw new IndexException(e)
    } finally lock.readLock.unlock()
  }

  /** @inheritdoc*/
  override def submitSynchronous[A](task:() => A): A = {
    lock.writeLock().lock()
    try {
      pools.map(_.submit(() => {})).foreach(_.get()) // Wait for queued tasks completion
      task.apply // Run synchronous task
    } catch {
      case e: InterruptedException =>
        logger.error("Task queue await interrupted", e)
        throw new IndexException(e)
      case e: ExecutionException =>
        logger.error("Task queue await failed", e)
        throw new IndexException(e)
      case e: Exception =>
        logger.error("Task queue synchronous submission failed", e)
        throw new IndexException(e)
    } finally lock.writeLock.unlock()
  }

  /** @inheritdoc*/
  override def close() = {
    lock.writeLock.lock()
    try pools.foreach(_.shutdown())
    finally lock.writeLock.unlock()
  }

}

object TaskQueue {

  val logger = LoggerFactory.getLogger(classOf[TaskQueue])

  /** Returns a new [[TaskQueue]].
    *
    * @param numThreads the number of executor threads
    * @param queuesSize the max number of tasks in each thread queue before blocking
    * @return a new task queue
    */
  def build(numThreads: Int, queuesSize: Int): TaskQueue = {
    if (numThreads > 0) new TaskQueueAsync(numThreads, queuesSize) else new TaskQueueSync()
  }

}
