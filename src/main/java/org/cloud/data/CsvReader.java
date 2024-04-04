package org.cloud.data;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;

public class CsvReader {

    public List<String> getHeaders(String url) {
        try {
            // Get CSV file content
            URL file = new URL(url);
            InputStream inputStream = file.openStream();

            // Read the CSV file using Apache Commons CSV
            CSVParser csvParser = CSVFormat.DEFAULT.parse(new InputStreamReader(inputStream));

            // Get headers
            return getHeaders(csvParser);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static List<String> getHeaders(CSVParser csvParser) {
        List<String> headers = new ArrayList<>();

        // Get the first record to determine headers
        Iterator<CSVRecord> csvRecordIterator = csvParser.iterator();
        if (csvRecordIterator.hasNext()) {
            CSVRecord firstRecord = csvRecordIterator.next();
            for (String header : firstRecord) {
                headers.add(header);
            }
        }
        return headers;
    }


    }


