package com.example.data_processor.service;

import com.example.data_processor.exception.TransformationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVReader;

@Service
public class CSVTransformationService {

    private static final Logger logger = LoggerFactory.getLogger(CSVTransformationService.class);

    @Value("${error.storage.path}") // Path to store error JSON files
    private String errorStoragePath;

    public List<Map<String, Object>> transformCSVToMySQL(File file) throws TransformationException {
        logger.debug("Transforming CSV file: {}", file.getName());
        List<Map<String, Object>> transformedData = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(file)))) {
            String[] headers = reader.readNext(); // Read headers
            if (headers == null) {
                throw new TransformationException("CSV file is empty or has no headers");
            }

            String[] row;
            while ((row = reader.readNext()) != null) {
                if (row.length != headers.length) {
                    throw new TransformationException("Row length does not match headers");
                }

                Map<String, Object> rowData = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    rowData.put(headers[i], row[i]);
                }
                transformedData.add(rowData);
            }
        } catch (IOException e) {
            logger.error("Error reading CSV file: {}", file.getName(), e);
            saveTransformedDataAsJson(transformedData, file.getName());
            throw new TransformationException("Error reading CSV file: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Error transforming CSV data: {}", file.getName(), e);
            saveTransformedDataAsJson(transformedData, file.getName());
            throw new TransformationException("Error transforming CSV data: " + e.getMessage());
        }
        logger.debug("CSV file transformed successfully: {}", file.getName());
        return transformedData;
    }

    private void saveTransformedDataAsJson(List<Map<String, Object>> transformedData, String fileName) {
        if (transformedData.isEmpty()) {
            logger.warn("No transformed data to save for file: {}", fileName);
            return;
        }

        File errorDir = new File(errorStoragePath);
        if (!errorDir.exists()) {
            if (!errorDir.mkdirs()) {
                logger.error("Failed to create error storage directory: {}", errorStoragePath);
                return;
            }
        }

        String jsonFileName = fileName.replace(".csv", "_error.json");
        File jsonFile = new File(errorDir, jsonFileName);
        try (FileWriter writer = new FileWriter(jsonFile)) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(writer, transformedData);
            logger.info("Transformed data saved as JSON for further investigation: {}", jsonFile.getAbsolutePath());
        } catch (IOException e) {
            logger.error("Failed to save transformed data as JSON: {}", jsonFile.getAbsolutePath(), e);
        }
    }
}