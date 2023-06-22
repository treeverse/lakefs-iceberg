package io.lakefs.iceberg;

import org.apache.commons.lang3.StringUtils;

public class Util {

    private Util() {}

    public static String GetPathFromURL(String lakeFSLocation){
        // return sub string after lakeFS ref
        return StringUtils.substring(lakeFSLocation, StringUtils.ordinalIndexOf(lakeFSLocation, "/", 4) + 1);
    }

}
