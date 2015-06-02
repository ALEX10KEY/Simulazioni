package it.polito.tdp.babs.model;

import java.time.LocalDateTime;

/**
 * JavaBean representing the `trip` table in the `babs` database.
 * 
 * @author Fulvio Corno
 *
 */
public class Trip {

	private int tripID ;
	private int duration ;
	
	private LocalDateTime startDate ;
	private String startStationName ;
	private int startStationID ;
	
	private LocalDateTime endDate ;
	private String endStationName ;
	private int endStationID ;
	
	private int bikeNum ;
	private String subscriptionType ;
	private Integer zipCode ;
	
	public Trip(int tripID, int duration, LocalDateTime startDate,
			String startStationName, int startStationID, LocalDateTime endDate,
			String endStationName, int endStationID, int bikeNum,
			String subscriptionType, Integer zipCode) {
		super();
		this.tripID = tripID;
		this.duration = duration;
		this.startDate = startDate;
		this.startStationName = startStationName;
		this.startStationID = startStationID;
		this.endDate = endDate;
		this.endStationName = endStationName;
		this.endStationID = endStationID;
		this.bikeNum = bikeNum;
		this.subscriptionType = subscriptionType;
		this.zipCode = zipCode;
	}
	public int getTripID() {
		return tripID;
	}
	public void setTripID(int tripID) {
		this.tripID = tripID;
	}
	public int getDuration() {
		return duration;
	}
	public void setDuration(int duration) {
		this.duration = duration;
	}
	public LocalDateTime getStartDate() {
		return startDate;
	}
	public void setStartDate(LocalDateTime startDate) {
		this.startDate = startDate;
	}
	public String getStartStationName() {
		return startStationName;
	}
	public void setStartStationName(String startStationName) {
		this.startStationName = startStationName;
	}
	public int getStartStationID() {
		return startStationID;
	}
	public void setStartStationID(int startStationID) {
		this.startStationID = startStationID;
	}
	public LocalDateTime getEndDate() {
		return endDate;
	}
	public void setEndDate(LocalDateTime endDate) {
		this.endDate = endDate;
	}
	public String getEndStationName() {
		return endStationName;
	}
	public void setEndStationName(String endStationName) {
		this.endStationName = endStationName;
	}
	public int getEndStationID() {
		return endStationID;
	}
	public void setEndStationID(int endStationID) {
		this.endStationID = endStationID;
	}
	public int getBikeNum() {
		return bikeNum;
	}
	public void setBikeNum(int bikeNum) {
		this.bikeNum = bikeNum;
	}
	public String getSubscriptionType() {
		return subscriptionType;
	}
	public void setSubscriptionType(String subscriptionType) {
		this.subscriptionType = subscriptionType;
	}
	public Integer getZipCode() {
		return zipCode;
	}
	public void setZipCode(Integer zipCode) {
		this.zipCode = zipCode;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + tripID;
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
		Trip other = (Trip) obj;
		if (tripID != other.tripID)
			return false;
		return true;
	}
	
	
}
