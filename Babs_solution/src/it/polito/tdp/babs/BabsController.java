package it.polito.tdp.babs;

import it.polito.tdp.babs.model.Model;
import it.polito.tdp.babs.model.Station;
import it.polito.tdp.babs.model.StationComparatorByName;
import it.polito.tdp.babs.model.Trip;

import java.net.URL;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ResourceBundle;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.ComboBox;
import javafx.scene.control.DatePicker;
import javafx.scene.control.TextArea;

public class BabsController {

	private Model model;

	@FXML
	private ResourceBundle resources;

	@FXML
	private URL location;

	@FXML
	private ComboBox<Station> boxPartenza;

	@FXML
	private TextArea txtResult;

	@FXML
	private ComboBox<Station> boxArrivo;

	@FXML
	private DatePicker pickData;

	@FXML
	void initialize() {
		assert boxPartenza != null : "fx:id=\"boxPartenza\" was not injected: check your FXML file 'Babs.fxml'.";
		assert txtResult != null : "fx:id=\"txtResult\" was not injected: check your FXML file 'Babs.fxml'.";
		assert boxArrivo != null : "fx:id=\"boxArrivo\" was not injected: check your FXML file 'Babs.fxml'.";
		assert pickData != null : "fx:id=\"pickData\" was not injected: check your FXML file 'Babs.fxml'.";

	}

	@FXML
	void doTempoMedio(ActionEvent event) {
		
		this.txtResult.clear();
		
		Station partenza = boxPartenza.getValue();
		Station arrivo = boxArrivo.getValue();

		// Error checking
		if (partenza == null || arrivo == null) {
			txtResult.appendText("Errore: selezionare due stazioni\n");
			return;
		}

		// Compute number of trips
		int numTrip = model.numTrip(partenza, arrivo);

		if (numTrip != 0) {
			// Average duration is meaningful if there are >0 trips
			double tMedio = model.tempoMedio(partenza, arrivo);

			txtResult.appendText(String.format(
					"Percorso %s -> %s: %d trip, tempo medio %.2f secondi\n",
					partenza.toString(), arrivo.toString(), numTrip, tMedio));

		} else {
			// No trips: don't compute nor print average time
			txtResult.appendText(String.format("Percorso %s -> %s: %d trip\n",
					partenza.toString(), arrivo.toString(), numTrip));
		}
	}

	@FXML
	void doDettaglio(ActionEvent event) {

		this.txtResult.clear();
		
		LocalDate data = model.controllaData(this.pickData.getValue());
		Station partenza = boxPartenza.getValue();
		Station arrivo = boxArrivo.getValue();

		if (partenza == null || arrivo == null) {
			txtResult.appendText("Errore: selezionare due stazioni\n");
			return;
		} else if ( data == null ) {
			this.txtResult.appendText("Errore: selezionare data ricerca !");
			return ;
		} else {
			
			List<Trip> trips = model.findTripsBetweenStationInDay(partenza, arrivo, data) ;
			
			if (trips.size() == 0)
				System.out.println("No trip.");
			
			for ( Trip t : trips )
				this.txtResult.appendText( String.format("Partenza &s - Arrivo %s", t.getStartDate(), t.getEndDate()));
			
		}
		
	}

	public void setModel(Model model) {
		this.model = model;
		
		/*
		List<Station> stations = model.getStations();
		Collections.sort(stations,new StationComparatorByName());
		*/
		
		// Get the list of stations and sort them in alphabetical order senza modificare l'ordine interno
		List<Station> stations = new ArrayList<>(model.getStations());
		Collections.sort(stations, new StationComparatorByName());

		// Populate drop-down menus
		boxPartenza.getItems().addAll(stations);
		boxArrivo.getItems().addAll(stations);
		
		this.pickData.setValue(model.primaDataDisponibile());
	}
}
