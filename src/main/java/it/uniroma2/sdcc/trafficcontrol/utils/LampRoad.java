package it.uniroma2.sdcc.trafficcontrol.utils;

public class LampRoad {
	private int id;
	private String road;
	
	public LampRoad(int id, String road) {
		this.id = id;
		this.road = road;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getRoad() {
		return road;
	}

	public void setRoad(String road) {
		this.road = road;
	}

}
