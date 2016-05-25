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

import java.util.concurrent.atomic.AtomicInteger

class Slot {
	
	// Data collection purpose
	Integer slotId
    
	
	Boolean shouldRelaunch = false
    AtomicInteger freshInstanceStartupTriesDone = new AtomicInteger(0)
    InstanceMetaData old = new InstanceMetaData()
    InstanceMetaData fresh
	
	/*
	 * Used in "Revert" Instance failure handling mode
	 * 
	 * Flag to check if ASG MinSize has decremented due to manual termination of Instances within ASG,
	 * in order to increment it back to original value after manually attaching replacement instance
	 */
	Boolean isMinSizeReduced = false

    Slot(com.amazonaws.services.ec2.model.Instance oldInstance) {
        old.instance = oldInstance
        old.state = InstanceState.initial
        replaceFreshInstance()
    }

    boolean inProgress() {
        (old.state != InstanceState.initial && fresh?.state != InstanceState.ready) && 
		(old.state != InstanceState.revertReady) &&
		(old.state != InstanceState.revertPending) &&
		(getCurrent().state != InstanceState.revertInit) &&
		(getCurrent().state != InstanceState.revertTerminated)
    }

    InstanceMetaData getCurrent() {
        fresh?.id ? fresh : old
    }

    Integer replaceFreshInstance() {
        fresh = new InstanceMetaData()
        old.resetTimer()
        freshInstanceStartupTriesDone.incrementAndGet()
    }
}
