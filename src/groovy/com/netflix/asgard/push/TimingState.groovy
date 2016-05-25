package com.netflix.asgard.push;

public enum TimingState {
	ArrivalForward(0), 
	DepartureForward(1), 
	ArrivalBackward(2),
	DepartureBackward(3);
	
	private final int index
	
	TimingState(int index) {
		this.index = index
	}
	
	public int getIndex() {
		return index
	}
}
