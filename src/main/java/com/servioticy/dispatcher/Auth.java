package com.servioticy.dispatcher;

import com.servioticy.datamodel.sensorupdate.SensorUpdate;
import com.servioticy.datamodel.serviceobject.SO;
import com.servioticy.datamodel.subscription.Subscription;

/**
 * Created by alvaro on 28/12/15.
 */
public class Auth {
    public static boolean check(SO so, SensorUpdate su){
        return true;
    }

    public static boolean check(Subscription subscription, SensorUpdate su){
        return true;
    }
}
