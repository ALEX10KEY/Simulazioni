package it.polito.tdp.babs.model;

import it.polito.tdp.babs.db.BabsDAO;

import java.time.LocalDate;
import java.util.List;

/**
 * The Model class for represeting the BABS data model
 * 
 * @author Fulvio Corno
 *
 */
public class Model {
	
	private List<Station> stations ;

	/**
	 * The list of all loaded {@link Station}s.
	 * @return all the {@link Station}s
	 */
	public List<Station> getStations() {
		return stations;
	}

	/**
	 * Load the list of all available {@link Station}s from the database. Must be called before the other methods.
	 */
	public void loadStations() {
		BabsDAO dao = new BabsDAO() ;
		
		stations = dao.getAllStations() ; 
	}

	/**
	 * Compute the number of trips between the two specified {@link Station} nodes, where a bike was taken
	 * at the first station, and returned at the second one.
	 * 
	 * @param partenza Start {@link Station} (departure)
	 * @param arrivo End {@link Station} (arrival)
	 * @return Number of trips
	 */
	public int numTrip(Station partenza, Station arrivo) {
		BabsDAO dao = new BabsDAO() ;
		int numTrip = dao.numTrip(partenza, arrivo) ;
		return numTrip ;
	}

	/**
	 * Compute the average duration (in seconds) of a trip between the two specified {@link Station} nodes.
	 * 
	 * @param partenza tart {@link Station} (departure)
	 * @param arrivo End {@link Station} (arrival)
	 * @return Average duration (in seconds) of the trips. If no trips are available, returns 0.0.
	 */
	public double tempoMedio(Station partenza, Station arrivo) {
		BabsDAO dao = new BabsDAO() ;
		double tempoMedio = dao.tempoMedioInJava(partenza, arrivo) ;
		return tempoMedio;
	}
	
	public LocalDate primaDataDisponibile() {
		BabsDAO dao = new BabsDAO() ;
		return dao.dataMinimaPossibile();
	}
	
	public LocalDate controllaData(LocalDate data) {
		BabsDAO dao = new BabsDAO() ;
		return dao.controllaValiditaData(data);
	}
	
	public List<Trip> findTripsBetweenStationInDay(Station start,Station end, LocalDate date) {
		BabsDAO dao = new BabsDAO() ;
		return dao.findTripsInDay(start, end, date);
	}

}
