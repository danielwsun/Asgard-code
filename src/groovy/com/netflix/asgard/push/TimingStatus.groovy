package com.netflix.asgard.push;

import java.text.SimpleDateFormat

public class TimingStatus {
	private Date time
	private Integer wave
	
	public TimingStatus(Date time, Integer wave) {
		this.time = time
		this.wave = wave
	}
	
	public String getTime() {
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss")
		return sdf.format(time)
	}
	
	public Integer getWave() {
		return wave
	}
}
