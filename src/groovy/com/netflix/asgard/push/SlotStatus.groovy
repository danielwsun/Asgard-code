package com.netflix.asgard.push;

import java.util.Date;

public class SlotStatus {
	String instanceId
	String event
	Integer waveNo
	Date time
	
	public SlotStatus(String instanceId, String event, Integer waveNo = null) {
		this.instanceId = instanceId
		this.event = event
		this.waveNo = waveNo
		this.time = new Date()
	}
}
