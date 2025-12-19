package org.iit.weather;

import java.io.*;
import java.util.*;

/**
 * PreprocessWeather
 * =================
 *
 * PURPOSE:
 * --------
 * This program prepares raw weather and location datasets
 * into a single, flattened CSV file that is easy to use with:
 *  - Hadoop MapReduce
 *  - Hive / Pig
 *  - Spark (SQL, MLlib)
 *
 * IMPORTANT DESIGN DECISION:
 * --------------------------
 * 1. NO time-based filtering is done here.
 * 2. The output dataset is GENERIC and REUSABLE.
 * 3. Any filtering (e.g., last decade) is applied at analysis stage.
 *
 * INPUT FILES:
 * ------------
 * data/weatherData.csv   -> raw weather observations
 * data/locationData.csv  -> location_id to city/district mapping
 *
 * OUTPUT FILE:
 * ------------
 * data/weather_preprocessed.csv
 *
 * OUTPUT SCHEMA:
 * --------------
 * district,
 * year,
 * month,
 * precipitation_hours,
 * temperature_2m_mean,
 * temperature_2m_max,
 * evapotranspiration,
 * shortwave_radiation,
 * sunshine_duration,
 * wind_speed_max
 *
 * This schema supports:
 *  - Task 1 (MapReduce analytics)
 *  - Task 2 (Hive aggregations)
 *  - Task 3 (Spark analytics & ML)
 */
public class PreprocessWeather {

    public static void main(String[] args) throws Exception {

        /* =========================================================
         * STEP 1: Load location_id → district (city_name) mapping
         * =========================================================
         *
         * The weather dataset contains only location_id.
         * We join it with the location dataset to get district names.
         */
        Map<String, String> locationMap = new HashMap<>();

        BufferedReader locReader =
                new BufferedReader(new FileReader("data/locationData.csv"));

        String line;
        locReader.readLine(); // Skip CSV header

        while ((line = locReader.readLine()) != null) {
            String[] parts = line.split(",");
            String locationId = parts[0];
            String districtName = parts[7]; // city_name column
            locationMap.put(locationId, districtName);
        }
        locReader.close();

        /* =========================================================
         * STEP 2: Read weather data and write flattened output
         * ========================================================= */
        BufferedReader weatherReader =
                new BufferedReader(new FileReader("data/weatherData.csv"));

        BufferedWriter writer =
                new BufferedWriter(new FileWriter("data/weather_preprocessed.csv"));

        /* Write CSV header (IMPORTANT for Hive & Spark) */
        writer.write(
            "district,year,month,precipitation_hours,temperature_2m_mean," +
            "temperature_2m_max,evapotranspiration,shortwave_radiation," +
            "sunshine_duration,wind_speed_max\n"
        );

        weatherReader.readLine(); // Skip weather CSV header

        while ((line = weatherReader.readLine()) != null) {

            String[] p = line.split(",");

            /* Join with location dataset */
            String district = locationMap.get(p[0]);
            if (district == null) continue; // Safety check

            /* Extract date fields (M/D/YYYY) */
            String[] date = p[1].split("/");
            int month = Integer.parseInt(date[0]);
            int year  = Integer.parseInt(date[2]);

            /* Write flattened record */
            writer.write(
                district + "," +
                year + "," +
                month + "," +
                p[13] + "," + // precipitation_hours
                p[5]  + "," + // temperature_2m_mean
                p[3]  + "," + // temperature_2m_max
                p[18] + "," + // evapotranspiration
                p[17] + "," + // shortwave_radiation
                p[9]  + "," + // sunshine_duration
                p[14] + "\n"  // wind_speed_max
            );
        }

        weatherReader.close();
        writer.close();
    }
}
