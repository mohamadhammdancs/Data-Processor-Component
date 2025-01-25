package com.example.data_processor.service;

import com.example.data_processor.util.MockMultipartFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

@Service
public class DLQConsumer {

    private static final Logger logger = LoggerFactory.getLogger(DLQConsumer.class);

    @Value("${dlq.dir}") // Use absolute path from properties
    private String dlqDir;

    @Value("${dlq.max.file.size:10485760}") // Default max file size: 10MB
    private long maxFileSize;

    public void sendToQueue(MultipartFile file, String sourceId, String error) {
        try {
            // Validate file size
            if (file.getSize() > maxFileSize) {
                logger.error("File size exceeds maximum allowed size: {}", file.getSize());
                throw new RuntimeException("File size exceeds maximum allowed size: " + file.getSize());
            }

            // Ensure the DLQ directory exists
            File dlqDirFile = new File(dlqDir);
            if (!dlqDirFile.exists()) {
                if (!dlqDirFile.mkdirs()) {
                    throw new RuntimeException("Failed to create DLQ directory: " + dlqDir);
                }
            }

            // Save the file to the DLQ
            String filename = dlqDir + "/" + sourceId + "_" + System.currentTimeMillis() + ".csv";
            File dlqFile = new File(filename);
            file.transferTo(dlqFile);
            logger.info("File saved to DLQ: {}", dlqFile.getAbsolutePath());

            // Save error details to a separate log file
            String errorLogFilename = filename.replace(".csv", "_error.log");
            try (FileWriter writer = new FileWriter(errorLogFilename)) {
                writer.write("Error: " + error + "\n");
                writer.write("Source ID: " + sourceId + "\n");
                writer.write("Original Filename: " + file.getOriginalFilename() + "\n");
            }
            logger.info("Error details saved to: {}", errorLogFilename);
        } catch (IOException e) {
            logger.error("Failed to write to DLQ: {}", e.getMessage());
            throw new RuntimeException("Failed to write to DLQ: " + e.getMessage());
        }
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
}
