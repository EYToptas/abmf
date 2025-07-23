package com.abmf.kafkaconsumer.service;

import com.abmf.kafkaconsumer.BalanceService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.util.concurrent.Executors;

@Service
public class LogFileTailService {

    private static final String LOG_FILE_PATH = "kafkaconsumer/lib/logs/kafka-app.log";

    @Autowired
    private BalanceService balanceService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void startTailingLogFile() {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                RandomAccessFile file = new RandomAccessFile(Paths.get(LOG_FILE_PATH).toFile(), "r");
                file.seek(file.length());

                System.out.println("Log takibi başlatıldı: " + LOG_FILE_PATH);

                while (true) {

                    String line = file.readLine();
                   // System.out.println(line);
                    if (line != null) {
                        handleLogLine(line);
                    } else {
                        Thread.sleep(10);
                    }
                }

            } catch (Exception e) {
                System.err.println("Log dosyası okunurken hata: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    private void handleLogLine(String line) {
        if (line.contains("Value: BalanceMessage{")) {
            try {
                String prefix = "Value: BalanceMessage{";
                int start = line.indexOf(prefix);
                int end = line.lastIndexOf("}");

                if (start != -1 && end != -1) {
                    String balancePart = line.substring(start + prefix.length(), end);

                    String[] fields = balancePart.split(",");

                    String msisdn = null;
                    int minutes = 0, sms = 0, data = 0;

                    for (String field : fields) {
                        String[] keyValue = field.trim().split("=");
                        if (keyValue.length == 2) {
                            switch (keyValue[0].trim()) {
                                case "msisdn":
                                    msisdn = keyValue[1].replace("'", "");
                                    break;
                                case "new_minutes":
                                    minutes = Integer.parseInt(keyValue[1]);
                                    break;
                                case "new_sms":
                                    sms = Integer.parseInt(keyValue[1]);
                                    break;
                                case "new_data":
                                    data = Integer.parseInt(keyValue[1]);
                                    break;
                            }
                        }
                    }

                    if (msisdn != null) {
                        balanceService.updateBalance(msisdn, minutes, sms, data);
                        System.out.println("✅ Oracle'a yazıldı: " + msisdn + " | Dakika: " + minutes + " | SMS: " + sms + " | Data: " + data);
                    }
                }

            } catch (Exception e) {
                System.err.println("❌ Parse hatası: " + e.getMessage());
            }
        } else {
            System.out.println("⏭️ Atlanan satır: " + line);
        }
    }

}
