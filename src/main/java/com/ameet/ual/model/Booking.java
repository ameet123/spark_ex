package com.ameet.ual.model;

/**
 * ameet.chaubal on 6/13/2019.
 */
public class Booking {
    private String bid;
    private String name;
    private String city;

    public Booking(String bid, String name, String city) {
        this.bid = bid;
        this.name = name;
        this.city = city;
    }

    public String getBid() {
        return bid;
    }

    public void setBid(String bid) {
        this.bid = bid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
