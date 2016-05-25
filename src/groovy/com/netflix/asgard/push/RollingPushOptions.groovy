/*
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.asgard.push

final class RollingPushOptions {
    CommonPushOptions common
    Integer relaunchCount
    Integer concurrentRelaunches
    Boolean newestFirst
    Boolean rudeShutdown
    String iamInstanceProfile
    String spotPrice
    String keyName
	
	/**
	 * Added function: specify the Rolling Push Mode.
	 * 
	 * - "synchronous":  execute Rolling Push by "waves". 
	 *                   Each "wave" will relaunch a number of nodes based on "concurrentRelaunches". 
	 *                   All instances during each "wave" need to be relaunched successfully 
	 *                   before going to the next "wave". 
	 *                   
	 * - "default":      Asgard default Rolling Push behaviour.
	 *                   Continuously relaunch instances based on "concurrentRelaunches".
	 *                   At any point in time during the upgrading process,
	 *                   there will be "concurrentRelaunches" number of instances undergoing relaunch.
	 */
	String rollingPushMode
	
	/**
	 * Added function: specify the Mode for handling instance failure during Rolling Push.
	 * 
	 * "Failure" means an up-and-running instance failing Health Check, or being stopped or terminated
	 * "unexpectedly" during the operation.
	 * 
	 * - "default":  Asgard default behaviour.
	 *               Spawn a replacement instance with the new Launch Configuration.
	 *                             
	 * - "revert":   spawn a replacement instance with the OLD Launch Configuration,
	 *               then add that instance to the Relaunch queue.
	 */
	String instanceFailureHandlingMode
	
	
    Boolean shouldWaitAfterBoot() {
        Math.max(0, common.afterBootWait) && !common.checkHealth
    }

    def propertyMissing(String name) { common[name] }
}
