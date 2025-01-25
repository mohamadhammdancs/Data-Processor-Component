package com.example.data_processor.service;

import com.example.data_processor.exception.InvalidCSVException;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;


@Service
public class CSVValidationService {

    private static final Logger logger = LoggerFactory.getLogger(CSVValidationService.class);

    public boolean validateCSVData(File file) throws InvalidCSVException {
        logger.debug("Validating CSV file: {}", file.getName());
        try (CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(file)))) {
            String[] headers = reader.readNext();
            String[] expectedHeaders = {"timestamp", "event_id", "source", "severity", "message", "user_id", "ip_address", "transaction_id"};

            if (headers == null || headers.length != expectedHeaders.length) {
                throw new InvalidCSVException("Invalid CSV headers in file: " + file.getName());
            }

            for (int i = 0; i < headers.length; i++) {
                if (!headers[i].equalsIgnoreCase(expectedHeaders[i])) {
                    throw new InvalidCSVException("Invalid header found: " + headers[i] + " in file: " + file.getName());
                }
            }

            int rowNum = 1;
            String[] row;
            while ((row = reader.readNext()) != null) {
                rowNum++;
                if (row.length != headers.length) {
                    throw new InvalidCSVException("Invalid row length at line " + rowNum + " in file: " + file.getName());
                }
            }
            logger.debug("CSV file validation successful.");
            return true;
        } catch (IOException e) {
            throw new InvalidCSVException("Error reading CSV file: " + e.getMessage());
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }
}
