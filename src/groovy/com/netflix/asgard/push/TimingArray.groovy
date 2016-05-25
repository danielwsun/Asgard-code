package com.netflix.asgard.push;

public class TimingArray {
	
	private final int statesToRecord = 4
	private int totalInstancesNumber
	
	TimingStatus[][] timingArray
	
	public TimingArray(Integer totalInstancesNumber) {
		this.totalInstancesNumber = totalInstancesNumber
		timingArray = new TimingStatus[totalInstancesNumber + 1][statesToRecord]
	}
	
	public void addTimingEvent(Integer numberUpgradedInstances, TimingState state, Date time, Integer waveNo) {
		timingArray[numberUpgradedInstances][state.getIndex()] = new TimingStatus(time, waveNo)
	}
	
	public String printTimingArray() {
		StringBuilder sb = new StringBuilder()
		sb.append(String.format("%-15s%-20s%-20s%-20s%-20s\n", 
			"No. upd svrs", 
			"Arrival Forward", "Departure Forward",
			"Arrival Backward", "Departure Backward"))
		for (int i = 0; i < 95; i++) {
			sb.append("-")
		}
		sb.append("\n")
		for (int i = 0; i <= totalInstancesNumber; i++) {
			Boolean canCrash = false
			sb.append(String.format("%-15d", i))
			for (int j = 0; j < statesToRecord; j++) {
				if (timingArray[i][j] != null) {
					sb.append(String.format("%-20s", "Wave " + timingArray[i][j].getWave()))
				} else {
					sb.append(String.format("%-20s", ""))
				}
			}
			
			sb.append("\n")
		}
		return sb.toString()
	}
	
	public boolean isSlotEmpty(Integer noUpgraded, TimingState state) {
		return (timingArray[noUpgraded][state.getIndex()] == null)
	}
}
