package com.netflix.asgard.push;

import java.util.Date;
import java.util.List;

public class RollingUpgradeWave {

	Integer id
	Date startTime
	Date endTime
	List<Integer> slotAttempted
	List<Integer> slotSucceded
	List<Integer> slotFailed
	List<Integer> operationFailures
	List<String> systemFailures
	
	public RollingUpgradeWave(Integer id) {
		this.id = id
		startTime = new Date()
		slotAttempted = new ArrayList<Integer>()
		slotSucceded = new ArrayList<Integer>()
		slotFailed = new ArrayList<Integer>()
		operationFailures = new ArrayList<Integer>()
		systemFailures = new ArrayList<String>()
	}
	
}
