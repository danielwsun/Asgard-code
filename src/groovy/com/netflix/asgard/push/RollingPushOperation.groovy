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

import java.text.SimpleDateFormat
import java.util.Collection;

import com.amazonaws.services.autoscaling.model.Activity
import com.amazonaws.services.autoscaling.model.AutoScalingGroup
import com.amazonaws.services.autoscaling.model.Instance
import com.amazonaws.services.autoscaling.model.LaunchConfiguration
import com.amazonaws.services.ec2.model.CreateTagsRequest
import com.amazonaws.services.ec2.model.Image
import com.amazonaws.services.ec2.model.Placement
import com.amazonaws.services.ec2.model.Reservation
import com.amazonaws.services.ec2.model.RunInstancesRequest
import com.amazonaws.services.ec2.model.RunInstancesResult
import com.amazonaws.services.ec2.model.Tag
import com.netflix.asgard.Ensure
import com.netflix.asgard.EntityType
import com.netflix.asgard.From
import com.netflix.asgard.Link
import com.netflix.asgard.Region
import com.netflix.asgard.Relationships
import com.netflix.asgard.Spring
import com.netflix.asgard.Time
import com.netflix.asgard.UserContext
import com.netflix.asgard.model.ApplicationInstance
import com.netflix.asgard.model.AutoScalingGroupBeanOptions
import com.netflix.asgard.model.AutoScalingGroupData
import com.netflix.asgard.model.AutoScalingProcessType
import com.netflix.asgard.model.LaunchConfigurationBeanOptions
import com.netflix.asgard.model.Subnets

import org.apache.commons.logging.LogFactory
import org.joda.time.Duration

/**
 * A complex operation that involves pushing an AMI image to many new instances within an auto scaling group, and
 * monitoring the status of the instances for real time user feedback.
 */
class RollingPushOperation extends AbstractPushOperation {
    private static final log = LogFactory.getLog(this)

	def imageService
    def awsEc2Service
    def configService
    def discoveryService
    def launchTemplateService
    def restClientService
    private List<Slot> relaunchSlots = []
    List<String> loadBalancerNames = []
    private final RollingPushOptions options
    PushStatus pushStatus
	
	/**
	 * Need to store old LC in this scope for the InstanceFailure Handling mode: Revert Mode
	 */
	LaunchConfiguration oldLC
	
	/**
	 * A "pool" of available slots to be relaunched. Only used in "synchronous" Rolling Upgrade mode.
	 */
	List<Slot> availableSlots
	
	/**
	 * The current RollingUpgrade "wave" - to be used in "synchronous" RollingUpgrade mode
	 */
	Integer wave
	
	// TODO data collection purpose - map to store the state change of each upgrade slot
	SlotMap slotMap
	TimingArray timingArray
	List<RollingUpgradeWave> waves
	
	// TODO Probability array of number of failures to be injected to each wave
	List<Double> failureProbs
	
	
    /**
     * If the timeout durations turn out to cause trouble in prod then this flag provides a way to prevent timing out.
     */
    static Boolean timeoutsEnabled = true

    RollingPushOperation(RollingPushOptions options) {
        this.options = options
        log.info "Autowiring services in RollingPushOperation instance"
        Spring.autowire(this)
    }

    String getTaskId() {
        task.id
    }

	/**
	 * Added function: new Rolling Push Execution Mode
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
    void start() {

        def thisPushOperation = this
        AutoScalingGroup group = awsAutoScalingService.getAutoScalingGroup(options.userContext, options.groupName)
        Image image = awsEc2Service.getImage(options.userContext, options.imageId)
        String appversion = image.getAppVersion()
        task = taskService.startTask(options.userContext, "Pushing $options.imageId " +
                (appversion ? "with package $appversion " : '') +
                "into group $options.groupName for app $options.appName", { task ->

            task.email = applicationService.getEmailFromApp(options.common.userContext, options.common.appName)

            // Store the new Task object in the RollingPushOperation
            thisPushOperation.task = task

            prepareGroup(group)
			
			// Call corresponding push method based on the specified execution mode
			if (options.rollingPushMode.equals("default")) {
				restartInstances(group)
			} else if (options.rollingPushMode.equals("synchronous")) {
				restartInstancesSynchronous(group)
			} else {
				throw new PushException("Invalid Rolling push Execution mode specified.")
			}
        }, Link.to(EntityType.autoScaling, options.groupName))
    }

    List<com.amazonaws.services.ec2.model.Instance> getSortedEc2Instances(List<Instance> asgInstances) {
        Closure sortAlgorithm = options.newestFirst ? { a, b -> b.launchTime <=> a.launchTime } : { it.launchTime }
        UserContext userContext = options.common.userContext
        List<com.amazonaws.services.ec2.model.Instance> ec2Instances =
                asgInstances.collect { awsEc2Service.getInstance(userContext, it.instanceId) }
                        .findAll { it != null }
                        .sort(sortAlgorithm)
        task.log("Sorted ${options.common.appName} instances in ${options.common.groupName} by launch time with " +
                "${options.newestFirst ? 'newest' : 'oldest'} first")
        ec2Instances
    }

	/**
	 * Added function: suspend RollingPush operation.
	 *
	 * "Suspended" state: Prevent deregistering and killing more instances (from the Initial Phase).
	 *                    However, the instances which are currently being upgraded (i.e. after Initial Phase) 
	 *                    will still continue to upgrade.
	 */
	void suspend() {
		// Only need to set the status of the task to "suspended". The behaviour is implemented
		// in HandleInitialPhase()
		task.status = 'suspended'
		task.log("Rolling push operation suspended. This state will prevent further old instances from being killed; however, instances which are currently in the process of upgrading will still be allowed to run to completion.")
	}
	
	/**
	 * Resume RollingPush operation from suspended state.
	 */
	void resume() {
		task.status = 'running'
		task.log("Rolling push is now resuming normal operation...")
	}
	
    private void prepareGroup(AutoScalingGroup group) {
		
		// Set the oldLC variable in Class scope if Revert mode is chosen
		if (options.instanceFailureHandlingMode.equals("revert")) {
			oldLC = awsAutoScalingService.getLaunchConfiguration(options.userContext,
                group.launchConfigurationName)
			task.log "Instance Failure Handling Mode: Revert Mode - when an instance fails unexpectedly, it will first be replaced by another instance with the OLD LaunchConfiguration, then that replacement will be added to the RollingPush queue"
		}
		
        LaunchConfiguration oldLaunch = awsAutoScalingService.getLaunchConfiguration(options.userContext,
                group.launchConfigurationName)
        String groupName = group.autoScalingGroupName
        loadBalancerNames = group.loadBalancerNames
        String newLaunchName = Relationships.buildLaunchConfigurationName(groupName)
        Time.sleepCancellably 100 // tiny pause before LC create to avoid rate limiting
        Collection<String> securityGroups = launchTemplateService.includeDefaultSecurityGroups(options.securityGroups,
                group.VPCZoneIdentifier as boolean, options.userContext.region)
        task.log("Updating launch from ${oldLaunch.launchConfigurationName} with ${options.imageId} into ${newLaunchName}")
        String iamInstanceProfile = options.iamInstanceProfile ?: null
        LaunchConfigurationBeanOptions launchConfig = new LaunchConfigurationBeanOptions(
                launchConfigurationName: newLaunchName, imageId: options.imageId, keyName: options.keyName,
                securityGroups: securityGroups, instanceType: options.instanceType,
                kernelId: oldLaunch.kernelId, ramdiskId: oldLaunch.ramdiskId, iamInstanceProfile: iamInstanceProfile,
                ebsOptimized: oldLaunch.ebsOptimized
        )
        UserContext userContext = options.common.userContext
        Subnets subnets = awsEc2Service.getSubnets(userContext)
        AutoScalingGroupBeanOptions groupForUserData = AutoScalingGroupBeanOptions.from(group, subnets)
        groupForUserData.launchConfigurationName = newLaunchName
        launchConfig.userData = launchTemplateService.buildUserData(options.common.userContext, groupForUserData,
                launchConfig)
        awsAutoScalingService.createLaunchConfiguration(options.common.userContext, launchConfig, task)

        Time.sleepCancellably 200 // small pause before ASG update to avoid rate limiting
        task.log("Updating group ${groupName} to use launch config ${newLaunchName}")
        final AutoScalingGroupData autoScalingGroupData = AutoScalingGroupData.forUpdate(
                groupName, newLaunchName,
                group.minSize, group.desiredCapacity, group.maxSize, group.defaultCooldown,
                group.healthCheckType, group.healthCheckGracePeriod, group.terminationPolicies, group.availabilityZones
        )
        awsAutoScalingService.updateAutoScalingGroup(options.common.userContext, autoScalingGroupData, [], [], task)
    }

	
	/**
	 * Default Asgard behaviour when dealing with unexpected instance failure during RollingPush.
	 *
	 * The sick/failed instance will be replaced by a new instance with the NEW LaunchConfiguration.
	 * (due to ASG autoscaling feature, since the ASG was already loaded with the new LC before executing relaunch)
	 * 
	 * 
	 * Added function - Unexpected Instance Failure Handling Mode: Revert Mode.
	 * 
	 * The sick/failed instance will first be replaced by an instance with the OLD LaunchConfiguration;
	 * then that replacement instance will be added to the RollingPush queue to be relaunched as a new instance with new LC.
	 */
    private void decideActionsForSlot(Slot slot) {

        // Stagger external requests by a few milliseconds to avoid rate limiting and excessive object creation
        Time.sleepCancellably 300

        // Are we acting on the old instance or the fresh instance?
        InstanceMetaData instanceInfo = slot.current

        // How long has it been since the targeted instance state changed?
        Duration timeSinceChange = instanceInfo.timeSinceChange

        Boolean timeForPeriodicLogging = instanceInfo.isItTimeForPeriodicLogging()

        Region region = options.common.userContext.region
        boolean discoveryExists = configService.doesRegionalDiscoveryExist(region)
        Duration afterDiscovery = discoveryExists ? discoveryService.timeToWaitAfterDiscoveryChange : Duration.ZERO

        UserContext userContext = options.common.userContext
		
		
		// Only do this check if the chosen InstanceFailureHandlingMode is "revert"
		if (options.instanceFailureHandlingMode.equals("revert")) {
			
			// Need to check whether an instance with new AMI spawned has already become InService
			// without Asgard knowing and matching with an upgrading slot
			AutoScalingGroup freshGroup = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
			
			// Stagger iterations to avoid AWS rate limiting
			Time.sleepCancellably 300
			
			def errorInst = freshGroup.instances.find {
				def instanceFromGroup = it
				def matchingSlot = relaunchSlots.find { theSlot ->
					instanceFromGroup.instanceId == theSlot.current.id
				}
				// AWS lifecycleState values are Pending, InService, Terminating and Terminated
				// http://docs.amazonwebservices.com/AutoScaling/latest/DeveloperGuide/CHAP_Glossary.html
				(instanceFromGroup?.lifecycleState?.equals("InService")) && !matchingSlot
			}
			if (errorInst) {
				// Get the EC2 Instance object based on the ASG Instance object
				com.amazonaws.services.ec2.model.Instance errorInstEc2 = awsEc2Service.getInstance(userContext, errorInst.instanceId)
				if (errorInstEc2) {
					if (!errorInstEc2.getState().getName().equals("shutting-down") &&
						 !errorInstEc2.getState().getName().equals("terminated")) {
						// Need to kill the found instance in order for the RollingUpgrade to continue
						task.log "REVERT: InService instance ${errorInstEc2.getInstanceId()} with new AMI found without association with any Slot in Asgard. Terminate that instance so that the Rolling Upgrade process can continue..."
						awsEc2Service.terminateInstances(userContext, [errorInstEc2.getInstanceId()], task)
					}
				}
			}
			
			
			/*
			 * Check whether the current instance fails unexpectedly
			 * TODO change to health check of instance
			 * Here "failure" is defined as:
			 * The instance status is either "stopping", "stopped", "shutting-down" or "terminated" when it shouldn't be,
			 * which is when the RollingPush state of the corresponding slot is NOT "terminated"
			 */
			com.amazonaws.services.ec2.model.Instance thisInstance = awsEc2Service.getInstance(userContext, instanceInfo.id)
			String ec2InstanceStatus = thisInstance.getState().getName()
			if ((instanceInfo.state != InstanceState.terminated) && (instanceInfo.state != InstanceState.revertTerminated)) {
				if (ec2InstanceStatus.equals("stopped") || ec2InstanceStatus.equals("terminated") ||
					ec2InstanceStatus.equals("stopping") || ec2InstanceStatus.equals("shutting-down")) {
					// Record the previous State, Set the current slot instance state to "revertInit" (if it didn't happen already)
					if (instanceInfo.state != InstanceState.revertInit) {
						instanceInfo.previousState = instanceInfo.state
						instanceInfo.state = InstanceState.revertInit
						
						// Remove the slot from the "pool" of available slots as well, if RollingPush mode is "synchronous"
						if (options.rollingPushMode.equals("synchronous")) {
							availableSlots.remove(slot)
						}
						
						// TODO data collection - Log the instance failure and when it happens
						task.log "COLLECT: Slot ${slot.slotId} - Instance ${instanceInfo.id} failed unexpectedly. The instance previous state is ${instanceInfo.previousState}"
						
						
						// TODO data collection - add InstanceFails event to SlotMap
						slotMap.addEvent(slot.slotId, "InstanceFails", wave, instanceInfo.id)

					}
				}
			}
		}

        switch (instanceInfo.state) {

            // Starting here we're looking at the state of the old instance
            case InstanceState.initial:
				// If push operation is currently suspended, skip this slot
				if (task.status == 'suspended') { break }
			
				// If the RollingUpgrade mode is "synchronous", only start the upgrade when slot.shouldRelaunch = true
				if (options.rollingPushMode.equals("synchronous")) {
					if (!slot.shouldRelaunch) {
						break
					}
				} 
				
                // If too many other slots are still in progress, then skip this unstarted slot for now.
                if (areTooManyInProgress()) { break }

                handleInitialPhase(instanceInfo, userContext, discoveryExists, afterDiscovery)
                break

            case InstanceState.unregistered:
                // Shut down abruptly or wait for clients of the instance to adjust to the change.
                handleUnregisteredPhase(timeSinceChange, afterDiscovery, instanceInfo, userContext)
                break

            case InstanceState.terminated:
                handleTerminatedPhase(userContext, instanceInfo, slot)
                break

            // After here instanceInfo holds the state of the fresh instance, not the old instance
            case InstanceState.pending:
                //if (state == InstanceState.pending) {
                handlePendingPhase(userContext, instanceInfo, timeForPeriodicLogging, slot)
                break

            case InstanceState.running:
                handleRunningPhase(userContext, instanceInfo)
                break

            case InstanceState.registered:
                handleRegisteredPhase(instanceInfo, timeForPeriodicLogging)
                break

            case InstanceState.snoozing:
                handleSnoozingPhase(timeSinceChange, instanceInfo, slot)
                break
				
			// Added function: push state when instance failed unexpectedly
			case InstanceState.revertInit:
				handleRevertInitPhase(userContext, instanceInfo, slot)
				break
			
			case InstanceState.revertTerminated:
				handleRevertTerminatedPhase(userContext, instanceInfo, slot)
				break
				
			case InstanceState.revertPending:
				handleRevertPendingPhase(userContext, instanceInfo, slot)
				break
				
			case InstanceState.revertReady:
				handleRevertReadyPhase(userContext, instanceInfo, slot)
				break
        }

        // If a change should have happened by now then something is wrong so stop the push.
        if (timeoutsEnabled && timeSinceChange.isLongerThan(instanceInfo.state.timeOutToExitState)) {
            String err = "${reportSummary()} Timeout waiting ${Time.format(timeSinceChange)} for ${instanceInfo.id} to progress "
            err += "from ${instanceInfo.state?.name()} state. "
            err += "(Maximum ${Time.format(instanceInfo.state.timeOutToExitState)} allowed)."
            fail(err)
        }
    }	

    private void handleInitialPhase(InstanceMetaData instanceInfo, UserContext userContext, boolean discoveryExists, Duration afterDiscovery) {
        // Disable the app in discovery and ELBs so that clients don't try to talk to it
        String appName = options.common.appName
        String appInstanceId = "${appName} / ${instanceInfo.id}"
        if (loadBalancerNames) {
            task.log("Disabling ${appInstanceId} in ${loadBalancerNames.size()} ELBs.")
            for (String loadBalancerName in loadBalancerNames) {
                awsLoadBalancerService.removeInstances(userContext, loadBalancerName, [instanceInfo.id], task)
            }
        }
        if (discoveryExists) {
            discoveryService.disableAppInstances(userContext, appName, [instanceInfo.id], task)
            if (options.rudeShutdown) {
                task.log("Rude shutdown mode. Not waiting for clients to stop using ${appInstanceId}")
            } else {
                task.log("Waiting ${Time.format(afterDiscovery)} for clients to stop using ${appInstanceId}")
            }
        }
        instanceInfo.state = InstanceState.unregistered
    }

    private void handleUnregisteredPhase(Duration timeSinceChange, Duration afterDiscovery, InstanceMetaData instanceInfo, UserContext userContext) {
        if (options.rudeShutdown || timeSinceChange.isLongerThan(afterDiscovery)) {
            task.log("Terminating instance ${instanceInfo.id}")
            if (awsEc2Service.terminateInstances(userContext, [instanceInfo.id], task) == null) {
                fail("${reportSummary()} Instance ${instanceInfo.id} failed to terminate. Aborting push.")
            }
            instanceInfo.state = InstanceState.terminated
            String duration = Time.format(InstanceState.terminated.timeOutToExitState)
            task.log("Waiting up to ${duration} for new instance of ${options.groupName} to become Pending.")
        }
    }

    private void handleTerminatedPhase(UserContext userContext, InstanceMetaData instanceInfo, Slot slot) {
        AutoScalingGroup freshGroup = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
        // See if new ASG instance can be found yet
        def newInst = freshGroup.instances.find {
            def instanceFromGroup = it
            def matchingSlot = relaunchSlots.find { theSlot ->
                instanceFromGroup.instanceId == theSlot.fresh.id
            }
            // AWS lifecycleState values are Pending, InService, Terminating and Terminated
            // http://docs.amazonwebservices.com/AutoScaling/latest/DeveloperGuide/CHAP_Glossary.html
            (instanceFromGroup?.lifecycleState?.equals("Pending")) && !matchingSlot
        }
        if (newInst) {
            // Get the EC2 Instance object based on the ASG Instance object
            com.amazonaws.services.ec2.model.Instance instance = awsEc2Service.getInstance(userContext, newInst.instanceId)
            if (instance) {
                task.log("It took ${Time.format(instanceInfo.timeSinceChange)} for instance ${instanceInfo.id} to terminate and be replaced by ${instance.instanceId}")
                slot.fresh.instance = instance
                slot.fresh.state = InstanceState.pending
                task.log("Waiting up to ${Time.format(InstanceState.pending.timeOutToExitState)} for Pending ${instance.instanceId} to go InService.")
            }
        }
    }

    private void handlePendingPhase(UserContext userContext, InstanceMetaData instanceInfo, boolean timeForPeriodicLogging, Slot slot) {
        def freshGroup = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
        def newInst = freshGroup.instances.find { it.instanceId == instanceInfo.id }
        String lifecycleState = newInst?.lifecycleState
        Boolean freshInstanceFailed = !newInst || "Terminated" == lifecycleState
        if (timeForPeriodicLogging || freshInstanceFailed) {
            task.log("Instance of ${options.appName} on ${instanceInfo.id} is in lifecycle state ${lifecycleState ?: 'Not Found'}")
        }

        if (freshInstanceFailed) {
            Integer startupTriesDone = slot.replaceFreshInstance()
            if (startupTriesDone > options.maxStartupRetries) {
                fail("${reportSummary()} Startup failed ${startupTriesDone} times for one slot. " +
                        "Max ${options.maxStartupRetries} tries allowed. Aborting push.")
            } else {
                task.log("Startup failed on ${instanceInfo.id} so Amazon terminated it. Waiting up to ${Time.format(InstanceState.terminated.timeOutToExitState)} for another instance.")
            }
        }

        // If no longer pending, change state
        if (newInst?.lifecycleState?.equals("InService")) {
            task.log("It took ${Time.format(instanceInfo.timeSinceChange)} for instance ${instanceInfo.id} to go from Pending to InService")
            instanceInfo.state = InstanceState.running
            if (options.checkHealth) {
                task.log("Waiting up to ${Time.format(InstanceState.running.timeOutToExitState)} for Eureka registration of ${options.appName} on ${instanceInfo.id}")
            }
        }
    }

    private void handleRunningPhase(UserContext userContext, InstanceMetaData instanceInfo) {
        if (options.checkHealth) {

            // Eureka isn't yet strong enough to handle sustained rapid fire requests.
            Time.sleepCancellably(discoveryService.MILLIS_DELAY_BETWEEN_DISCOVERY_CALLS as Integer)

            ApplicationInstance appInst = discoveryService.getAppInstance(userContext,
                    options.common.appName, instanceInfo.id)
            if (appInst) {
                task.log("It took ${Time.format(instanceInfo.timeSinceChange)} for instance " +
                        "${instanceInfo.id} to go from InService to registered with Eureka")
                instanceInfo.state = InstanceState.registered
                def healthCheckUrl = appInst.healthCheckUrl
                if (healthCheckUrl) {
                    instanceInfo.healthCheckUrl = healthCheckUrl
                    task.log("Waiting up to ${Time.format(InstanceState.registered.timeOutToExitState)} for health check pass at ${healthCheckUrl}")
                }
            }
        }
        // If check health is off, prepare for final wait
        else {
            startSnoozing(instanceInfo)
        }
    }

    private void handleRegisteredPhase(InstanceMetaData instanceInfo, boolean timeForPeriodicLogging) {
        // If there's a health check URL then check it before preparing for final wait
        if (instanceInfo.healthCheckUrl) {
            Integer responseCode = restClientService.getRepeatedResponseCode(instanceInfo.healthCheckUrl)
            String message = "Health check response code is ${responseCode} for application ${options.appName} on instance ${instanceInfo.id}"
            if (responseCode >= 400) {
                fail("${reportSummary()} ${message}. Push was aborted because ${instanceInfo.healthCheckUrl} " +
                        "shows a sick instance. See http://go/healthcheck for guidelines.")
            }
            Boolean healthy = responseCode == 200
            if (timeForPeriodicLogging || healthy) {
                task.log(message)
            }
            if (healthy) {
                task.log("It took ${Time.format(instanceInfo.timeSinceChange)} for instance " +
                        "${instanceInfo.id} to go from registered to healthy")
                startSnoozing(instanceInfo)
            }
        }
        // If there is no health check URL, assume instance is ready for final wait
        else {
            task.log("Can't check health of ${options.appName} on ${instanceInfo.id} since no URL is registered.")
            startSnoozing(instanceInfo)
        }
    }

    private void handleSnoozingPhase(Duration timeSinceChange, InstanceMetaData instanceInfo, Slot slot) {
        Boolean ready = true
        if (options.shouldWaitAfterBoot()
                && timeSinceChange.isShorterThan(Duration.standardSeconds(options.common.afterBootWait))) {
            ready = false
        }
        if (ready) {
            instanceInfo.state = InstanceState.ready
            task.log("Instance ${options.appName} on ${instanceInfo.id} is ready for use. ${reportSummary()}")
			
			
			// TODO data collection - add "NewVersionReady" event to SlotMap
			slotMap.addEvent(slot.slotId, "NewVersionReady", wave, instanceInfo.id)
			
			
        }
    }

	
	/**
	 * Only triggered if InstanceFailureHandlingMode = "revert"
	 * 
	 * Handle the phase when an instance which fail unexpectedly is detected
	 */
	private void handleRevertInitPhase(UserContext userContext, InstanceMetaData instanceInfo, Slot slot) {
				
		// Need to check whether any "Launch" or "Attach" instance scaling activities are currently in progress;
		// if any is still in progress, skip this slot for now to avoid error
		// (since there might be risk of stalling the entire rolling push process)
		if (isLaunchScalingActivitiesInProgress()) {
			return
		}	
		// NOTE: need to suspend various AutoScaling Processes first to prevent them from popping up before executing ASG operations
		awsAutoScalingService.suspendProcess(userContext, AutoScalingProcessType.ReplaceUnhealthy, options.groupName, task)
		awsAutoScalingService.suspendProcess(userContext, AutoScalingProcessType.Launch, options.groupName, task)
		
			
		// Get the current ASG
		AutoScalingGroup group = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
		// Store the original MinSize
		Integer oldMinSize = group.getMinSize()
		
		// Check if the instance is currently registered with the ASG
		Boolean isInstanceWithinAutoScalingGroup = false
		String ASGInstanceState = ""
		List<Instance> instances = group.getInstances()
		for (Instance instance : instances) {
			if (instance.getInstanceId().equals(instanceInfo.id)) {
				isInstanceWithinAutoScalingGroup = true
				ASGInstanceState = instance.getLifecycleState()
				break
			}
		}
		
		// If the instance is currently registered with the ASG
		if (isInstanceWithinAutoScalingGroup) {
			
			/* If the failed instance has already been detected by ASG beforehand and is currently
			 * terminating, skip this and come back later
			 */
			if (ASGInstanceState.equals("Terminating")) {
				
				task.log "REVERT: SKIP - Failed instance ${instanceInfo.id} has been detected by the ASG and it's currently terminating."
				
				// NOTE: now done messing with the ASG. Can now resume the suspended AutoScaling processes
				awsAutoScalingService.resumeProcess(userContext, AutoScalingProcessType.ReplaceUnhealthy, options.groupName, task)
				awsAutoScalingService.resumeProcess(userContext, AutoScalingProcessType.Launch, options.groupName, task)
				
				return
			}
			
			
			/* Kill and deregister the failed instance from the ASG if it hasn't been killed yet.
			 *
			 * Also, decrement the desired capacity of the ASG (and probably minSize as well),
			 * since the attachInstances() later on will increment the desired instances number.
			 *
			 * This also prevent ASG to scale new instance to replace this one
			 * (but probably still need to check just in case the replacement is already been spawned before this)
			 */
			awsAutoScalingService.terminateInstanceAndDecrementAutoScalingGroup(userContext, instanceInfo.id)
			
			// Stagger the operation for few milliseconds
			Time.sleepCancellably 300
			
			AutoScalingGroup newGroup = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
			// Determine whether the ASG MinSize has changed because of the operation
			Integer newMinSize = newGroup.getMinSize()
			if (oldMinSize != newMinSize) {
				slot.isMinSizeReduced = true
			}
			
		} else {
			/* Else, the instance is not registered with the ASG.
			 *
			 * 3 possible cases:
			 *  - A replacement instance with the old LC failed before attached to the ASG
			 *    (in this case, just need to terminate the failed EC2 instance)
			 *
			 *  - A new instance being relaunched by ASG autoscaling function failed before going InService.
			 *    After the instance fails, ASG will automatically deregister the instanceId;
			 *    however, the ASG DesiredCapacity remains the same.
			 *    (in this case, in addition to terminating the failed instance,
			 *     also need to decrement the DesiredCapacity and probably MinSize)
			 *     
			 *  - An unexpectedly failed instance not being dealt with promptly and get detected and terminated by ASG.
			 *    The ASG will deregister that instance, but will not decrease the DesiredCapacity.
			 *    (same resolution as 2nd case)
			 */
		
			// Terminate the failed EC2 instance
			awsEc2Service.terminateInstances(userContext, [instanceInfo.id], task)
			
			
			// Need to decrement the DesiredCapacity (and MinSize as well, if MinSize = DesiredCapacity)
			// for case no.2 and no.3 above
			if (instanceInfo.previousState != InstanceState.revertPending) {
				group = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
				Boolean isInstanceNotAssociatedWithASlot = false
				Instance instanceNotAssociatedWithASlot
				instances = group.getInstances()
				for (Instance instance : instances) {
					if (instance.getLifecycleState().equals("InService")) {
						int i
						for (i = 0; i < relaunchSlots.size(); i++) {
							if (instance.getInstanceId().equals(relaunchSlots[i].current.id)) {
								break
							}
						}
						if (i == relaunchSlots.size()) {
							isInstanceNotAssociatedWithASlot = true
							instanceNotAssociatedWithASlot = instance
							break
						}
					}
				}
				
				if (isInstanceNotAssociatedWithASlot) {
					
					// Need to double check for any InProgress Launch activities here, since it might get triggered
					if (isLaunchScalingActivitiesInProgress()) {
						task.log "REVERT: skip this slot for now, Launch scaling activities in progress..."
						// NOTE: now done messing with the ASG. Can now resume the suspended AutoScaling processes
						awsAutoScalingService.resumeProcess(userContext, AutoScalingProcessType.ReplaceUnhealthy, options.groupName, task)
						awsAutoScalingService.resumeProcess(userContext, AutoScalingProcessType.Launch, options.groupName, task)
						return
					}
					
					task.log "REVERT: failed instance ${instanceInfo.id} already has replacement ${instanceNotAssociatedWithASlot.getInstanceId()} unassociated with Asgard. Terminating replacement..."
					
					awsAutoScalingService.terminateInstanceAndDecrementAutoScalingGroup(
						userContext, instanceNotAssociatedWithASlot.getInstanceId())
					
					// Stagger the operation for few milliseconds
					Time.sleepCancellably 300
					
					AutoScalingGroup newGroup = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
					// Determine whether the ASG MinSize has changed because of the operation
					Integer newMinSize = newGroup.getMinSize()
					if (oldMinSize != newMinSize) {
						slot.isMinSizeReduced = true
					}
					
					// Need to wait for the replacement instance to terminate
					com.amazonaws.services.ec2.model.Instance ec2insNotAssociated = awsEc2Service.getInstance(
						userContext, instanceNotAssociatedWithASlot.getInstanceId())
					instanceInfo.instance = ec2insNotAssociated
					
				} else {
					
					group = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
					if (group.getInstances().size() < group.getDesiredCapacity()) {
						
						task.log "REVERT: Decrement ASG DesiredCapacity..."
						Integer intendedDesiredCapacity = group.desiredCapacity - 1
						
						if (group.getDesiredCapacity() <= group.getMinSize()) {
							final AutoScalingGroupData autoScalingGroupData = AutoScalingGroupData.forUpdate(
								group.autoScalingGroupName, group.launchConfigurationName,
								group.minSize - 1, group.desiredCapacity - 1, group.maxSize, group.defaultCooldown,
								group.healthCheckType, group.healthCheckGracePeriod, group.terminationPolicies,
								group.availabilityZones)
							awsAutoScalingService.updateAutoScalingGroup(userContext, autoScalingGroupData)	
							slot.isMinSizeReduced = true
						} else {
							final AutoScalingGroupData autoScalingGroupData = AutoScalingGroupData.forUpdate(
								group.autoScalingGroupName, group.launchConfigurationName,
								group.minSize, group.desiredCapacity - 1, group.maxSize, group.defaultCooldown,
								group.healthCheckType, group.healthCheckGracePeriod, group.terminationPolicies,
								group.availabilityZones)
							awsAutoScalingService.updateAutoScalingGroup(userContext, autoScalingGroupData)
						}
						
						
						// Small loop to make sure the DesiredCapacity is properly updated b4 continue
						while (true) {
							group = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
							if (group.getDesiredCapacity() == intendedDesiredCapacity) {
								break
							}
							// Stagger iterations to avoid AWS rate limiting and excessive object creation
							Time.sleepCancellably 500
						}	
					} else {
						// Skip
						task.log "REVERT: SKIP - revertInit() for failed instance ${instanceInfo.id} - currentCapacity=${group.getInstances().size()}, desiredCapacity=${group.getDesiredCapacity()}"
					
						// NOTE: now done messing with the ASG. Can now resume the suspended AutoScaling processes
						awsAutoScalingService.resumeProcess(userContext, AutoScalingProcessType.ReplaceUnhealthy, options.groupName, task)
						awsAutoScalingService.resumeProcess(userContext, AutoScalingProcessType.Launch, options.groupName, task)
					
						return
					}
				}	
			}
		}
		
		
		// NOTE: now done messing with the ASG. Can now resume the suspended AutoScaling processes
		awsAutoScalingService.resumeProcess(userContext, AutoScalingProcessType.ReplaceUnhealthy, options.groupName, task)
		awsAutoScalingService.resumeProcess(userContext, AutoScalingProcessType.Launch, options.groupName, task)
		
		// Change state to revertTerminated
		task.log "REVERT: Waiting for failed instance ${instanceInfo.id} to fully terminate."
		slot.old.resetTimer()
		slot.fresh.resetTimer()
		instanceInfo.state = InstanceState.revertTerminated
	}
	
	/**
	 * Only triggered if InstanceFailureHandlingMode = "revert"
	 * 
	 * Manually launching a replacement instance with the old AMI
	 */
	private void handleRevertTerminatedPhase(UserContext userContext, InstanceMetaData instanceInfo, Slot slot) {
		// Need to check whether the current failed instance has already been terminated from the ASG;
		// i.e. the instance has been deregistered from ASG, and the ASG DesiredCapacity has been reduced
		if (awsAutoScalingService.isInstanceWithinAutoScalingGroup(userContext, options.groupName, instanceInfo.id)) {
			// Skip this for now if the termination of the failed instance has not been completed
			return
		}
		
		task.log "REVERT: It took ${Time.format(instanceInfo.timeSinceChange)} for failed instance ${instanceInfo.id} to fully terminate."
		
		// Spawn a replacement instance with the old AMI
		List<com.amazonaws.services.ec2.model.Instance> replacements = imageService.runOnDemandInstance(userContext,
			oldLC.imageId, oldLC.securityGroups, oldLC.instanceType, oldLC.keyName)
		com.amazonaws.services.ec2.model.Instance replacementInstance = replacements.get(0)
		
		task.log "REVERT: Failed instance ${instanceInfo.id} is replaced with instance ${replacementInstance.getInstanceId()} which has old AMI."
		task.log "REVERT: Waiting up to ${Time.format(InstanceState.revertPending.timeOutToExitState)} for replacement instance ${replacementInstance.getInstanceId()} to be ready."
		
		// We want the slot to now act on the Old instance, since the replacement instance is OLD LaunchConfiguration
		slot.fresh = new InstanceMetaData()
		slot.old.resetTimer()
		// Associate the replaced instance with the current old Slot
		slot.old.instance = replacementInstance
		// Change the instance state to "revertPending"
		slot.old.state = InstanceState.revertPending
	}
	
	/**
	 * Only triggered if InstanceFailureHandlingMode = "revert"
	 *
	 * Handle the phase during which the replacement instance with the old LC is "pending"
	 */
	private void handleRevertPendingPhase(UserContext userContext, InstanceMetaData instanceInfo, Slot slot) {
		// Check for the state of the replacement instance
		com.amazonaws.services.ec2.model.Instance thisInstance = awsEc2Service.getInstance(userContext, instanceInfo.id)
		String ec2InstanceStatus = thisInstance.getState().getName()
		// If it's ready
		if (ec2InstanceStatus.equals("running")) {
			
			// Need to check whether any "Launch" or "Attach" instance scaling activities are currently in progress;
			// if any is still in progress, skip this slot for now to avoid error
			// (since there might be risk of stalling the entire rolling push process)
			if (isLaunchScalingActivitiesInProgress()) {
				return
			}
			// NOTE: need to suspend various AutoScaling Processes first to prevent them from popping up before executing ASG operations
			awsAutoScalingService.suspendProcess(userContext, AutoScalingProcessType.ReplaceUnhealthy, options.groupName, task)

			
			task.log "REVERT: It took ${Time.format(instanceInfo.timeSinceChange)} for replacement instance ${instanceInfo.id} with the old AMI to be ready."
			
			// Store the current DesiredCapacity
			AutoScalingGroup group = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
			Integer oldDesiredCapacity = group.getDesiredCapacity()
			
			// Attach the instance to the ASG
			// Need to attach at this point (i.e. when the instance is ready) in order not to confuse Asgard when matching relaunched instance
			// since Asgard will find pending instances inside the ASG
			awsAutoScalingService.attachInstanceToAutoScalingGroup(userContext, options.groupName, instanceInfo.id)
			
			// Keep looping until the LifeCycle state of the attached instance is NOT Pending anymore
			// => in order not to confuse Asgard to match this instance to a relaunch instance
			Boolean isFailedReplacement = false
			
			while (true) {
				group = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
				Instance replacementInstance = null
				for (Instance instance : group.instances) {
					if (instance.getInstanceId().equals(instanceInfo.id)) {
						replacementInstance = instance
						break
					}
				}
				if (replacementInstance != null) {
					if (!replacementInstance.getLifecycleState().equals("Pending")) {
						break
					}
				} else {
					isFailedReplacement = true
					break
				}
				// Stagger iterations to avoid AWS rate limiting and excessive object creation
				Time.sleepCancellably 500
			}
			
			// If the replacement instance failed to attach to the ASG
			if (isFailedReplacement) {
				// Check if the DesiredCapacity has increased due to the failed attachment process
				group = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
				Integer newDesiredCapacity = group.getDesiredCapacity()
				if (newDesiredCapacity != oldDesiredCapacity) {
					// If it is, need to revert the DesiredCapacity to the previous value
					final AutoScalingGroupData autoScalingGroupData = AutoScalingGroupData.forUpdate(
						group.autoScalingGroupName, group.launchConfigurationName,
						group.minSize, oldDesiredCapacity, group.maxSize, group.defaultCooldown,
						group.healthCheckType, group.healthCheckGracePeriod, group.terminationPolicies,
						group.availabilityZones)
					awsAutoScalingService.updateAutoScalingGroup(userContext, autoScalingGroupData)
				}
				
				
				// Small loop to make sure the DesiredCapacity is properly updated b4 continue
				while (true) {
					group = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
					if (group.getDesiredCapacity() == oldDesiredCapacity) {
						break
					}
					// Stagger iterations to avoid AWS rate limiting and excessive object creation
					Time.sleepCancellably 500
				}
				
				
				// NOTE: now done messing with the ASG. Can now resume the suspended AutoScaling processes
				awsAutoScalingService.resumeProcess(userContext, AutoScalingProcessType.ReplaceUnhealthy, options.groupName, task)

				
				// Quit this so the instance failure can be detected in next iteration
				return
			}
			
			// Increment the AutoScalingGroup MinSize if it's been decremented since the last operation
			if (slot.isMinSizeReduced) {
				Time.sleepCancellably 300
				group = checkGroupStillExists(userContext, options.groupName, From.AWS_NOCACHE)
				final AutoScalingGroupData autoScalingGroupData = AutoScalingGroupData.forUpdate(
					group.autoScalingGroupName, group.launchConfigurationName,
					group.minSize + 1, group.desiredCapacity, group.maxSize, group.defaultCooldown,
					group.healthCheckType, group.healthCheckGracePeriod, group.terminationPolicies,
					group.availabilityZones)
				awsAutoScalingService.updateAutoScalingGroup(userContext, autoScalingGroupData)
				
				slot.isMinSizeReduced = false
			}
			
			
			// NOTE: now done messing with the ASG. Can now resume the suspended AutoScaling processes
			awsAutoScalingService.resumeProcess(userContext, AutoScalingProcessType.ReplaceUnhealthy, options.groupName, task)
	
			
			
			task.log "REVERT: Replacement instance '${instanceInfo.id}' with old AMI has been attached to the ASG, and is now ready for relaunch."	
			
			
			// TODO data collection - add "OldVersionReplacementReady" event to SlotMap
			slotMap.addEvent(slot.slotId, "OldVersionReplacementReady", wave, instanceInfo.id)
			
			
			instanceInfo.state = InstanceState.revertReady
		}
	}
	
	private void handleRevertReadyPhase(UserContext userContext, InstanceMetaData instanceInfo, Slot slot) {
		// Only restore the slot state to Initial if rolling push mode is NOT synchronous
		if (!options.rollingPushMode.equals("synchronous")) {
			instanceInfo.state = InstanceState.initial
		}
	}
	
    private void fail(String message) {
        // Refresh AutoScalingGroup and Cluster cache objects when push fails.
        awsAutoScalingService.getAutoScalingGroup(options.userContext, options.groupName)
        throw new PushException(message)
    }

    private String reportSummary() {
        replacePushStatus()
        return "${pushStatus.countReadyNewInstances()} of ${options.relaunchCount} instance relaunches done."
    }

    private void startSnoozing(InstanceMetaData instanceInfo) {
        if (options.shouldWaitAfterBoot()) {
            task.log("Waiting ${options.afterBootWait} second${options.afterBootWait == 1 ? "" : "s"} for ${options.appName} on ${instanceInfo.id} to be ready.")
        }
        instanceInfo.state = InstanceState.snoozing
    }

    private boolean areTooManyInProgress() {
        Integer concurrentRelaunches = Math.min(options.concurrentRelaunches, relaunchSlots.size())
        def slotsInProgress = relaunchSlots.findAll { it.inProgress() }
        return slotsInProgress.size() >= concurrentRelaunches
    }

	/**
	 * Check whether the last few scaling activities performed by the ASG is still in progress or not
	 * @param numActivities the number of recent scaling activities to check; defaulted to last 3 activities
	 * @return TRUE if any of the recent scaling activities is still in progress; false otherwise
	 */
	private boolean isScalingActivitiesInProgress(Integer numActivitiesToCheck = 3) {
		List<Activity> activities = awsAutoScalingService.getAutoScalingGroupActivities(
			options.userContext, options.groupName, numActivitiesToCheck)
		if (!activities.isEmpty()) {
			for (Activity lastScalingActivity : activities) {
				if (!(lastScalingActivity.getStatusCode().equals("Successful") ||
					lastScalingActivity.getStatusCode().equals("Failed") ||
					lastScalingActivity.getStatusCode().equals("Cancelled"))) {
						return true
				}
			}
		}
		return false
	}
	
	
	/**
	 * Check whether there are any "LAUNCH" or "ATTACH" instances scaling activities which are still in progress
	 * @return TRUE if any "LAUNCH" or "ATTACH" instances scaling activities are still in progress; false otherwise
	 */
	private boolean isLaunchScalingActivitiesInProgress() {
		List<Activity> activities = awsAutoScalingService.getAutoScalingGroupActivities(
			options.userContext, options.groupName, 100)
		if (!activities.isEmpty()) {
			for (Activity activity : activities) {
				if (activity.getDescription().matches("Launch.*") || 
					    activity.getDescription().matches("Attach.*")) {
					if (!(activity.getStatusCode().equals("Successful") ||
					          activity.getStatusCode().equals("Failed") ||
					          activity.getStatusCode().equals("Cancelled"))) {
						return true
					}
				}
			}
		}
		return false
	}
	
	
    /**
     * Default RollingPush behaviour.
     * 
     * Continuously relaunching instances based on number of concurrent relaunches.
     */
    private void restartInstances(AutoScalingGroup group) {

        List<com.amazonaws.services.ec2.model.Instance> ec2Instances = getSortedEc2Instances(group.instances)

        // Create initial slots
        ec2Instances.each { relaunchSlots << new Slot(it) }
        for (Integer i = 0; i < options.relaunchCount; i++) {
            relaunchSlots[i].shouldRelaunch = true
        }
        Integer total = ec2Instances.size()
        task.log "The group ${options.common.groupName} has ${total} instance${total == 1 ? "" : "s"}. " +
                "${options.relaunchCount} will be replaced, ${options.concurrentRelaunches} at a time."

        // Iterate through the slots that are marked for relaunch and choose the next action to take for each one.
        Boolean allDone = false
        while ((!allDone) && ((task.status == "running") || (task.status == 'suspended'))) {
            relaunchSlots.findAll { it.shouldRelaunch }.each { Slot slot -> decideActionsForSlot(slot) }
            replacePushStatus()
            allDone = pushStatus.allDone

            // Stagger iterations to avoid AWS rate limiting and excessive object creation
            Time.sleepCancellably 1000
        }

        // Refresh AutoScalingGroup and Cluster cache objects when push succeeds.
        awsAutoScalingService.getAutoScalingGroup(options.userContext, options.groupName)
    }

	
	/**
	 * "Synchronous" RollingPush behaviour.
	 * 
	 * Execute RollingPush by "waves", each wave will relaunch instances based on number of concurrent relaunches.
	 * Must wait for a "wave" to finish relaunching before going to another "wave".
	 */
	private void restartInstancesSynchronous(AutoScalingGroup group) {
		
		// TODO AZRebalance will mess up the process. disable it here
		awsAutoScalingService.suspendProcess(options.common.userContext, AutoScalingProcessType.AZRebalance, options.groupName, task)
		
		// TODO Random number generator for Operation Failure injection
		Random rand = new Random(System.currentTimeMillis())
		Random randSysFailure = new Random(System.currentTimeMillis())
		Random randSysCategory = new Random(System.currentTimeMillis())
		
		// TODO initialize the probability array for System Failure injection
		failureProbs = new ArrayList<Double>()
		failureProbs.add(0.4)
		failureProbs.add(0.57)
		failureProbs.add(0.7)
		failureProbs.add(1.0)
		
		
		// TODO Running experiment purpose - print current Task ID
		task.log "TASKID: ${task.id}"
		
		
		List<com.amazonaws.services.ec2.model.Instance> ec2Instances = getSortedEc2Instances(group.instances)

		// Create a "pool" of available Slots to be upgraded
		availableSlots = new ArrayList<Slot>()
		
		// Create initial slots
		ec2Instances.each { relaunchSlots << new Slot(it) }
		for (Integer i = 0; i < options.relaunchCount; i++) {
			
			// TODO Data collection - Set the Slot ID for each slot 
			relaunchSlots[i].slotId = i
			
			
			// This time we want NO slot to be marked for relaunch initially
			relaunchSlots[i].shouldRelaunch = false
			// Add this slot to the available Slot pool
			availableSlots.add(relaunchSlots[i])
		}
		
		// TODO data collection - initialise SlotMap
		slotMap = new SlotMap(relaunchSlots)
		timingArray = new TimingArray(options.relaunchCount)
		Integer previousNoUpgraded = 0
		String direction = "forward"
		waves = new ArrayList<RollingUpgradeWave>()
		
		
		Integer total = ec2Instances.size()
		task.log "RollingPush execution mode selected: synchronous"
		task.log "The group ${options.common.groupName} has ${total} instance${total == 1 ? "" : "s"}. " +
				"${options.relaunchCount} will be replaced. The operation will relaunch instances in \"waves\", " + 
				"with a maximum of ${options.concurrentRelaunches} instances to be relaunched for each wave."
		
		Boolean allDone = false		
		// Start from 1st wave
		wave = 1
		
		while ((!allDone) && ((task.status == "running") || (task.status == 'suspended'))) {
			
			
			// If the task state is suspended, just loop over the entire Slots List (to deal with instances failure)
			if (task.status == "suspended") {
				relaunchSlots.each { Slot slot -> decideActionsForSlot(slot) }
				
				// Stagger iterations to avoid AWS rate limiting and excessive object creation
				Time.sleepCancellably 1000
				
				continue
			}
			
			
			if (!availableSlots.isEmpty()) {
					
							
				// TODO data collection - record wave start time
				Date waveStartTime = new Date()
				waves.add(new RollingUpgradeWave(wave))
				
				
				// Mark up to <concurrentRelaunches> slots to be relaunched during current wave
				// but DON'T remove the slot from the AvailablePool yet!
				Iterator<Slot> it = availableSlots.iterator()
				int upgCount = 0
				while (it.hasNext() && upgCount < options.concurrentRelaunches) {
					Slot slot = it.next()
					upgCount++
					
					
					// TODO add the upgrading slot to waveArray
					waves.last().slotAttempted.add(slot.slotId)
					
					
					slot.shouldRelaunch = true
				}
				
				
				// TODO Count the number of upgraded, notUpgraded and ToBeUpgraded (i.e. Upgrading) during this wave
				Double upgradedProb
				Double notUpgradedProb
				Double upgradingProb
				int upgradingCount = 0
				int upgradedCount = 0
				int notUpgradedCount = 0
				relaunchSlots.findAll { it.shouldRelaunch }.each { upgradingCount++ }
				for (Slot s : relaunchSlots) {
					if (s.current.state == InstanceState.ready) {
						upgradedCount++
					} else {
						notUpgradedCount++
					}
				}
				notUpgradedCount -= upgradingCount
				
				// Calculate the fail probabilities of each category of server
				upgradedProb = upgradedCount / relaunchSlots.size()
				notUpgradedProb = notUpgradedCount / relaunchSlots.size()
				upgradingProb = upgradingCount / relaunchSlots.size()
				
				// Create a probability array for the 3 server categories
				List<Double> categoryProbs = new ArrayList<Double>()
				categoryProbs.add(upgradedProb)
				categoryProbs.add(upgradedProb + notUpgradedProb)
				categoryProbs.add(upgradedProb + notUpgradedProb + upgradingProb)
				
				// Roll dice for number of system failure to occur during this wave
				Double sysFailDice = randSysFailure.nextDouble()
				int numSysFailures = 0
				if (sysFailDice <= failureProbs[0]) {
					numSysFailures = 0
				} else {
					for (int i = 1; i < failureProbs.size(); i++) {
						if (sysFailDice > failureProbs[i-1] && sysFailDice <= failureProbs[i]) {
							numSysFailures = i
							break
						}
					}
				}
				// DEBUGGG!!!!
				task.log "${upgradedCount} ${notUpgradedCount} ${upgradingCount}, ${sysFailDice}, ${numSysFailures}"
				
				// Make a copy of the RelaunchSlots array
				List<Slot> relaunchSlotsCopy = new ArrayList<Slot>()
				for (Slot s : relaunchSlots) {
					relaunchSlotsCopy.add(s)
				}
				// Shuffle the copy
				Collections.shuffle(relaunchSlotsCopy)
				
				// For each System Failures to be inject
				for (int i = 0; i < numSysFailures; i++) {
					// Roll a dice to choose what server category will we inject the failure to
					Double categoryDice = randSysCategory.nextDouble()
					String categoryToInject = ""
					if (categoryDice <= categoryProbs[0]) {
						categoryToInject = "Upgraded"
						Slot slotToKill = null
						for (Slot s : relaunchSlotsCopy) {
							if (s.current.state == InstanceState.ready) {
								slotToKill = s
								break
							}
						}
						if (slotToKill != null) {
							relaunchSlotsCopy.remove(slotToKill)
							awsEc2Service.terminateInstances(
								options.common.userContext, [slotToKill.current.instance.instanceId], task)
							waves.last().systemFailures.add("${slotToKill.slotId} - ${categoryToInject}")
							task.log "INJECT: Slot ${slotToKill.slotId} - ${categoryToInject} has been killed!"
						}
					} else if (categoryDice > categoryProbs[0] && categoryDice <= categoryProbs[1]) {
						categoryToInject = "NotUpgraded"
						Slot slotToKill = null
						for (Slot s : relaunchSlotsCopy) {
							if ((s.current.state != InstanceState.ready) && (!s.shouldRelaunch)) {
								slotToKill = s
								break
							}
						}
						if (slotToKill != null) {
							relaunchSlotsCopy.remove(slotToKill)
							awsEc2Service.terminateInstances(
								options.common.userContext, [slotToKill.current.instance.instanceId], task)
							waves.last().systemFailures.add("${slotToKill.slotId} - ${categoryToInject}")
							task.log "INJECT: Slot ${slotToKill.slotId} - ${categoryToInject} has been killed!"
						}
					} else {
						categoryToInject = "Upgrading"
						Slot slotToKill = null
						for (Slot s : relaunchSlotsCopy) {
							if (s.shouldRelaunch) {
								slotToKill = s
								break
							}
						}
						if (slotToKill != null) {
							relaunchSlotsCopy.remove(slotToKill)
							awsEc2Service.terminateInstances(
								options.common.userContext, [slotToKill.current.instance.instanceId], task)
							waves.last().systemFailures.add("${slotToKill.slotId} - ${categoryToInject}")
							task.log "INJECT: Slot ${slotToKill.slotId} - ${categoryToInject} has been killed!"
						}
					}
					// DEBUGGGGG!
					StringBuilder sb = new StringBuilder()
					for (Slot s : relaunchSlotsCopy) {
						sb.append("${s.slotId},")
					}
					task.log "${sb.toString()}"
					task.log "${categoryProbs[0]} ${categoryProbs[1]} ${categoryProbs[2]}, ${categoryDice}, ${categoryToInject}"
				}
				
				
				// Remove the slots to be upgraded during this wave from the AvailableSlots Pool 
				relaunchSlots.findAll { it.shouldRelaunch }.each {
					Slot slot ->
					
					// TODO need to check if the slot has already been killed by System Failure Injection
					// Only consider Operation Failure if System Failure did not occur
					if (relaunchSlotsCopy.findAll { it == slot }.empty) {
						availableSlots.remove(slot)
						//DEBUGGG!
						task.log "INJECT: Slot ${slot.slotId} will have a SYSTEM FAILURE! Don't consider injecting Operation Failure on this"
					} else {
					
						// TODO inject Operation failure for each instance based on given probability
						Double operationSuccessRate = 0.9
						Double prob = rand.nextDouble()
						if (prob <= operationSuccessRate) {
							// Remove the slot from available pool and Relaunch that slot as normal
							availableSlots.remove(slot)
						} else {
							// Mark this slot as operation failure, i.e. don't do anything to this slot, and keep it in the available pool
							slot.current.state = InstanceState.revertReady
							// Log the operation failure
							task.log "COLLECT: Wave ${wave}: Operation failure will occur on slot ${slot.slotId} with instance Id ${slot.current.id}"
							waves.last().operationFailures.add(slot.slotId)
						}
					}
				}
			
				
				// Print wave no. and the instanceIds to be relaunched during this wave
				String instancesToBeRelaunched = ""
				relaunchSlots.findAll { it.shouldRelaunch }.each { Slot slot -> instancesToBeRelaunched += "${slot.current.id}; " }
				task.log "COLLECT: Wave ${wave} starting - Relaunching instances: ${instancesToBeRelaunched}"			
				
				// Do Rolling Upgrade on current wave
				while (!currentWaveDone(relaunchSlots)) {
					relaunchSlots.each { Slot slot -> decideActionsForSlot(slot) }
					
					// Stagger iterations to avoid AWS rate limiting and excessive object creation
					Time.sleepCancellably 1000
				}
				
				
				// TODO Data collection - Print wave result status
				waves.last().endTime = new Date()
				printWaveResult(wave)
				Integer currentNoUpgraded = relaunchSlots.findAll { it.current.state == InstanceState.ready }.size()
				if (currentNoUpgraded > previousNoUpgraded) {
					if (timingArray.isSlotEmpty(currentNoUpgraded, TimingState.ArrivalForward)) {
						timingArray.addTimingEvent(currentNoUpgraded, TimingState.ArrivalForward, new Date(), wave)
						direction = "forward"
					}
				} else if (currentNoUpgraded < previousNoUpgraded) {
					if (timingArray.isSlotEmpty(currentNoUpgraded, TimingState.ArrivalBackward)) {
						timingArray.addTimingEvent(currentNoUpgraded, TimingState.ArrivalBackward, new Date(), wave)
						direction = "backward"
					}
				} else {
					direction = "none"
				}
				if (direction.equals("forward")) {
					if (timingArray.isSlotEmpty(previousNoUpgraded, TimingState.DepartureForward)) {
						timingArray.addTimingEvent(previousNoUpgraded, TimingState.DepartureForward, waveStartTime, wave)
					}
				} else if (direction.equals("backward")) {
					if (timingArray.isSlotEmpty(previousNoUpgraded, TimingState.DepartureBackward)) {
						timingArray.addTimingEvent(previousNoUpgraded, TimingState.DepartureBackward, waveStartTime, wave)
					}
				}
				previousNoUpgraded = currentNoUpgraded
				//task.log "\n${timingArray.printTimingArray()}"
				
				
				
				// When current wave is done, disable relaunch for all slots in current wave 
				relaunchSlots.findAll { it.shouldRelaunch }.each { Slot slot -> slot.shouldRelaunch = false }
				
				// Check if all done
				replacePushStatus()
				allDone = pushStatus.allDone
				
				// Check all the slots for RevertReady instances
				for (Slot slot : relaunchSlots) {
					if (slot.current.state == InstanceState.revertReady) {
						// Add the slot back to AvailableSlots pool if it's not there
						// (to be scheduled for upgrade again)
						if (!availableSlots.contains(slot)) {
							availableSlots.add(slot)
						}
						// Set the state of the slot to Initial
						slot.current.state = InstanceState.initial
					}
				}
				
				// Go to next wave
				wave++
				
				// TODO Stagger iterations to accomodate logstash triggering of log line
				Time.sleepCancellably 1000
			} 
			/*
			 * Else, there are no Slot left available for upgrade, but the RollingUpgrade process is still not done.
			 * => There are still instances which failed unexpectedly, but their old AMI replacements haven't come back yet
			 * 
			 * Need to wait until at least 1 replacement come back successfully 
			 * (i.e. at least 1 slot changes its state to "revertReady")
			 */
			else {
				
				// Keep looping until at least 1 failed instance is successfully replaced with an instance with old AMI
				task.log "INFO: The Rolling Upgrade operation is not done yet; however there is currently no instance which is ready to be upgraded. It is most likely because unexpectedly failed instances are not coming back yet. Waiting for at least 1 instance to come back..."
				while (relaunchSlots.findAll { it.current.state == InstanceState.revertReady }.size() == 0) {
					relaunchSlots.each { Slot slot -> decideActionsForSlot(slot) }
					// Stagger iterations to avoid AWS rate limiting and excessive object creation
					Time.sleepCancellably 1000
				}
				
				// Check all the slots for RevertReady instances
				for (Slot slot : relaunchSlots) {
					if (slot.current.state == InstanceState.revertReady) {
						// Add the slot back to AvailableSlots pool if it's not there
						// (to be scheduled for upgrade again)
						if (!availableSlots.contains(slot)) {
							availableSlots.add(slot)
						}
						// Set the state of the slot to Initial
						slot.current.state = InstanceState.initial
					}
				}
			}
        }
		
		task.log "\n${timingArray.printTimingArray()}"
		printWavesDataResult()
		
		// TODO data collection - print table of Slot states
		task.log "${slotMap.printMap()}"
		

		// TODO Re-enable AZREbalance when RollingUpgrade done
		awsAutoScalingService.resumeProcess(options.common.userContext, AutoScalingProcessType.AZRebalance, options.groupName, task)
		
		
		// Refresh AutoScalingGroup and Cluster cache objects when push succeeds.
		awsAutoScalingService.getAutoScalingGroup(options.userContext, options.groupName)
	}
	
	
	/**
	 * Only used in "synchronous" RollingPush mode - execute RollingPush by "waves".
	 *
	 * Check whether the current "upgrade wave" is done -
	 * i.e. all instances scheduled to be relaunched during this wave are either "ready" or "revertReady"
	 *
	 * @return true if current wave is done, false otherwise
	 */
	private static boolean currentWaveDone(List<Slot> relaunchSlots) {
		for (Slot slot : relaunchSlots) {
			if (slot.shouldRelaunch) {
				if ((slot.current.state != InstanceState.ready)
					&& (slot.current.state != InstanceState.revertReady)) {
					return false
				}
			}
		}
		return true
	}
	
	
	// TODO Data collection - print wave result
	private void printWaveResult(Integer waveNo) {
		Integer totalAttempted = relaunchSlots.findAll { it.shouldRelaunch }.size()
		String success = ""
		Integer countSuccess = 0
		String failed = ""
		Integer countFailed = 0
		relaunchSlots.findAll { it.shouldRelaunch }.each {
			Slot slot -> 
			if (slot.current.state == InstanceState.ready) {
				countSuccess++
				success += "slot ${slot.slotId} with new instanceId ${slot.current.id}; "
				waves.last().slotSucceded.add(slot.slotId)
			} else if (slot.current.state == InstanceState.revertReady) {
				countFailed++
				failed += "slot ${slot.slotId} with old replacement instanceId ${slot.current.id}; "
				waves.last().slotFailed.add(slot.slotId)
			}
		}
		task.log "COLLECT: Wave ${waveNo} completed. Attempted to upgrade ${totalAttempted} slots." 
		task.log "COLLECT: ${countSuccess} slot(s) upgraded: ${success}"
		task.log "COLLECT: ${countFailed} slot(s) failed: ${failed}"
	}
	
	// TODO Data collection - print waves data collect result
	private void printWavesDataResult() {
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss")
		StringBuilder sb = new StringBuilder()
		sb.append(String.format("%-8s | %-10s | %-8s  |  %-14s  |  %-13s  |  %-11s  |  %-18s  |  %-68s  |  %-17s  |  %-17s\n",
			"Wave No.", "Start Time", "End Time",
			"Slot Attempted", "Slot Upgraded", "Slot Failed", 
			"Operation Failures", "System Failures",
			"Total OpsFailures", "Total SysFailures"))
		for (int i = 0; i < 216; i++) {
			sb.append("-")
		}
		sb.append("\n")
		Integer totalOpsFailures = 0
		Integer totalSysFailures = 0
		for (RollingUpgradeWave wave : waves) {
			totalOpsFailures += wave.operationFailures.size()
			totalSysFailures += wave.systemFailures.size()
			sb.append(String.format("%-8d | %-10s | %-8s  |  %-14s  |  %-13s  |  %-11s  |  %-18s  |  %-68s  |  %-17d  |  %-17d\n", 
				wave.id, sdf.format(wave.startTime), sdf.format(wave.endTime),
				wave.slotAttempted.join(","), wave.slotSucceded.join(","), wave.slotFailed.join(","),
				wave.operationFailures.join(","), wave.systemFailures.join(","),
				wave.operationFailures.size(), wave.systemFailures.size()))
		}
		for (int i = 0; i < 225; i++) {
			sb.append("-")
		}
		sb.append("\n")
		sb.append(String.format("%181s     %-17d  |  %-17d", "Totals:", totalOpsFailures, totalSysFailures))
		task.log "\n${sb.toString()}"
	}
	
	
    private void replacePushStatus() {
        pushStatus = new PushStatus(relaunchSlots, options)
    }
}
