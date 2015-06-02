package it.polito.tdp.babs.model;

import java.time.LocalDate;

public class Station implements Comparable<Station> {
	
	private int stationID ;
	private String name ;
	private double lat ;
	private double lon ;
	private int dockCount ;
	private String landmark ;
	private LocalDate installation ;
	
	public Station(int stationID, String name, double lat, double lon,
			int dockCount, String landmark, LocalDate installation) {
		super();
		this.stationID = stationID;
		this.name = name;
		this.lat = lat;
		this.lon = lon;
		this.dockCount = dockCount;
		this.landmark = landmark;
		this.installation = installation;
	}

	public int getStationID() {
		return stationID;
	}

	public void setStationID(int stationID) {
		this.stationID = stationID;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public int getDockCount() {
		return dockCount;
	}

	public void setDockCount(int dockCount) {
		this.dockCount = dockCount;
	}

	public String getLandmark() {
		return landmark;
	}

	public void setLandmark(String landmark) {
		this.landmark = landmark;
	}

	public LocalDate getInstallation() {
		return installation;
	}

	public void setInstallation(LocalDate installation) {
		this.installation = installation;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + stationID;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Station other = (Station) obj;
		if (stationID != other.stationID)
			return false;
		return true;
	}
	
	public String toString() {
		return this.name ;
	}

	@Override
	public int compareTo(Station other) {
		// return this.name.compareTo(other.getName()) ;
		return other.stationID - this.stationID ;
	}


}
