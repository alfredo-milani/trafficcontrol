package it.uniroma2.sdcc.trafficcontrol.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StringUtils {

    public static List<String> fromStringToList(String string) {
        return fromStringToList(string, ",");
    }

    public static List<String> fromStringToList(String string, String separator) {
        return new ArrayList<>(Arrays.asList(string
                .replace(" ", "")
                .replace("  ", "")
                .split(separator)));
    }

}
