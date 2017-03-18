package it.uniroma2.ember.stats;

import it.uniroma2.ember.utils.StreetLamp;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements a simple object to represent a ranking of the lamp to be replaced
 */
public class EmberLampLifeSpanRank {

    protected int count = 0;
    private List<StreetLamp> lamps = new ArrayList<>();

    public void incrementCount() {
        this.count += 1;
    }

    public void addLamp(StreetLamp streetLamp) {
        this.lamps.add(streetLamp);
    }

    public EmberLampLifeSpanRank() { /* */ }
}
