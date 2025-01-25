package com.example.data_processor.service;

import com.example.data_processor.exception.InvalidCSVException;
import com.example.data_processor.model.SystemLog;
import com.example.data_processor.repository.SystemLogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

@Service
public class DataProcessorService {

    private static final Logger logger = LoggerFactory.getLogger(DataProcessorService.class);

    @Autowired
    private SystemLogRepository systemLogRepository;

    @Autowired
    private CSVValidationService csvValidationService;

    @Autowired
    private CSVTransformationService csvTransformationService;

    @Autowired
    private DLQConsumer dlqConsumer;

    @Value("${temp.storage.path}")
    private String tempStoragePath;

    @Value("${dlq.dir}")
    private String dlqDir;

    @Scheduled(fixedRate = 24 * 60 * 60 * 1000) // Run once a day
    public void cleanUpTempFiles() {
        logger.info("Starting cleanup of temporary files in: {}", tempStoragePath);
        File tempDir = new File(tempStoragePath);
        if (tempDir.exists() && tempDir.isDirectory()) {
            File[] files = tempDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && file.lastModified() < System.currentTimeMillis() - (24 * 60 * 60 * 1000)) {
                        logger.info("Deleting file: {}", file.getName());
                        if (file.delete()) {
                            logger.debug("Successfully deleted file: {}", file.getName());
                        } else {
                            logger.error("Failed to delete file: {}", file.getName());
                        }
                    }
                }
            }
        }
        logger.info("Cleanup completed.");
    }

    @Scheduled(fixedRate = 24 * 60 * 60 * 1000) // Run once a day
    public void cleanUpDLQFiles() {
        logger.info("Starting cleanup of DLQ files in: {}", dlqDir);
        File dlqDirFile = new File(dlqDir);
        if (dlqDirFile.exists() && dlqDirFile.isDirectory()) {
            File[] files = dlqDirFile.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && file.lastModified() < System.currentTimeMillis() - (7 * 24 * 60 * 60 * 1000)) { // Delete files older than 7 days
                        logger.info("Deleting DLQ file: {}", file.getName());
                        if (file.delete()) {
                            logger.debug("Successfully deleted DLQ file: {}", file.getName());
                        } else {
                            logger.error("Failed to delete DLQ file: {}", file.getName());
                        }
                    }
                }
            }
        }
        logger.info("DLQ cleanup completed.");
    }

    @Retryable(value = {IOException.class, InvalidCSVException.class}, maxAttempts = 3, backoff = @Backoff(delay = 2000))
    public void ingestCSVData(MultipartFile file) throws Exception {
        logger.info("Processing uploaded file: {}", file.getOriginalFilename());
        File tempFile = null;
        try {
            tempFile = storeTempData(file);
            logger.debug("Temporary file saved at: {}", tempFile.getAbsolutePath());

            // Step 1: Validate the CSV file
            if (!csvValidationService.validateCSVData(tempFile)) {
                throw new InvalidCSVException("Invalid CSV file");
            }

            // Step 2: Transform the CSV data
            List<Map<String, Object>> transformedData = csvTransformationService.transformCSVToMySQL(tempFile);

            // Step 3: Save the transformed data directly to the database
            saveLogData(transformedData);

        } catch (InvalidCSVException e) {
            logger.warn("CSV validation failed for file: {}. Error: {}", file.getOriginalFilename(), e.getMessage());
            dlqConsumer.sendToQueue(file, "validation_error", e.getMessage()); // Send to DLQ
            throw e;
        } catch (Exception e) {
            logger.error("Error processing file: {}", file.getOriginalFilename(), e);
            dlqConsumer.sendToQueue(file, "processing_error", e.getMessage()); // Send to DLQ
            throw e;
        } finally {
            // Ensure file cleanup after processing
            if (tempFile != null && !tempFile.delete()) {
                logger.error("Failed to delete temporary file: {}", tempFile.getName());
            }
        }
    }

    private File storeTempData(MultipartFile file) throws IOException {
        logger.debug("Saving uploaded file to temporary storage: {}", file.getOriginalFilename());

        File tempDir = new File(tempStoragePath);
        if (!tempDir.exists()) {
            if (!tempDir.mkdirs()) {
                logger.error("Failed to create temporary directory: {}", tempStoragePath);
                throw new IOException("Failed to create temporary directory: " + tempStoragePath);
            }
        }

        // Create a temporary file
        File tempFile = new File(tempDir, file.getOriginalFilename());
        try {
            file.transferTo(tempFile); // Save the uploaded file to the temporary location
            logger.debug("File saved to temporary location: {}", tempFile.getAbsolutePath());
        } catch (IOException e) {
            logger.error("Failed to save uploaded file to temporary location: {}", tempFile.getAbsolutePath(), e);
            throw e;
        }
        return tempFile;
    }

    public void saveLogData(List<Map<String, Object>> logData) {
        logger.debug("Saving log data to the database. Rows: {}", logData.size());
        for (Map<String, Object> row : logData) {
            SystemLog log = new SystemLog();
            log.setTimestamp(ZonedDateTime.parse((String) row.get("timestamp")));
            log.setEventId((String) row.get("event_id"));
            log.setSource((String) row.get("source"));
            log.setSeverity((String) row.get("severity"));
            log.setMessage((String) row.get("message"));
            log.setUserId((String) row.get("user_id"));
            log.setIpAddress((String) row.get("ip_address"));
            log.setTransactionId((String) row.get("transaction_id"));
            log.setProcessedAt(ZonedDateTime.now()); // Add processed_at timestamp
            systemLogRepository.save(log);
        }
        logger.info("Log data saved to the database. Rows: {}", logData.size());
    }
}