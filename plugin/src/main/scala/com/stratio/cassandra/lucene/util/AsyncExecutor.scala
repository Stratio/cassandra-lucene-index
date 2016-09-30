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

/**
  * A queue that executes each submitted task using one of possibly several pooled threads. Tasks can be submitted with
  * an identifier, ensuring that all tasks with same identifier will be executed orderly in the same thread. Each thread
  * has its own task queue.
  *
  * @param numThreads the number of executor threads
  * @param queuesSize the max number of tasks in each thread queue before blocking
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class AsyncExecutor(numThreads: Int, queuesSize: Int) {

  val queue = new TaskQueue(numThreads, queuesSize)

  /**
    * Submits a non value-returning task for asynchronous execution.
    *
    * The specified identifier is used to choose the thread executor where the task will be queued. The selection and
    * load balancing is based in the hashcode of the supplied id.
    *
    * @param id   the identifier of the task used to choose the thread executor where the task will be queued for
    *             asynchronous execution
    * @param task the task to be queued for asynchronous execution
    */
  def submitAsynchronous(id: AnyRef, task: => Unit): Unit = {
    queue.submitAsynchronous(id, new Runnable {
      override def run(): Unit = task
    })
  }

  /**
    * Submits a non value-returning task for synchronous execution. It waits for all synchronous tasks to be
    * completed.
    *
    * @param task a task to be executed synchronously
    */
  def submitSynchronous(task: => Unit): Unit = {
    queue.submitSynchronous(new Runnable {
      override def run(): Unit = task
    })
  }

  def shutdown(): Unit = {
    queue.shutdown()
  }
}
