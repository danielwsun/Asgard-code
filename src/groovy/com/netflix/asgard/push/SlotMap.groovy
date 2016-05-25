package com.netflix.asgard.push;

import java.text.SimpleDateFormat
import java.util.ArrayList;
import java.util.List;


public class SlotMap {
	List<List<SlotStatus>> slotMap = new ArrayList<List<SlotStatus>>()
			
	public SlotMap(List<Slot> slots) {
		for (Slot slot : slots) {
			List<SlotStatus> element = new ArrayList<SlotStatus>()
			element.add(new SlotStatus(slot.current.id, "initial"))
			slotMap.add(element)
		}
	}
	
	public void addEvent(Integer slotId, String event, Integer waveNo, String instanceId) {
		slotMap[slotId].add(new SlotStatus(instanceId, event, waveNo))
	}
	
	public String printMap() {
		String output = ""
		for (int i = 0; i < slotMap.size(); i++) {
			output += "Slot Id ${i}: "
			for (SlotStatus ss : slotMap[i]) {
				SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss")
				output += "${df.format(ss.time)}:${ss.waveNo}:${ss.event}:${ss.instanceId}; "
			}
			output += "\n"
		}
		return output
	}
}
