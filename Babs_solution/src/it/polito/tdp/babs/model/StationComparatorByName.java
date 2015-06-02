package it.polito.tdp.babs.model;

import java.util.Comparator;

/**
 * Compare two {@link Station} beans by name
 * @author Fulvio Corno
 *
 */
public class StationComparatorByName implements Comparator<Station> {

	@Override
	public int compare(Station arg0, Station arg1) {
		return arg0.getName().compareTo(arg1.getName()) ;
	}

}
