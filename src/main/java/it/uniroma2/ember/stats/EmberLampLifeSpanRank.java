package it.uniroma2.ember.stats;

/**
 * Created by federico on 18/03/17.
 */

import it.uniroma2.ember.EmberInput;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements a simple object to represent a ranking of the lamp to be replaced
 */
public class EmberLampLifeSpanRank {

    protected int count = 0;
    private List<EmberInput.StreetLamp> lamps = new ArrayList<>();

    public void incrementCount() {
        this.count += 1;
    }

    public void addLamp(EmberInput.StreetLamp streetLamp) {
        this.lamps.add(streetLamp);
    }

    public EmberLampLifeSpanRank() { /* */ }
}
