package com.abmf.kafkaconsumer;


import ch.qos.logback.core.model.processor.ModelInterpretationContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
public class AbmfApplication implements CommandLineRunner {
	//ch.qos.logback.core.model.processor.ModelInterpretationContext.setConfiguratorSupplier()
	@Autowired
	private BalanceService balanceService;

	public static void main(String[] args) {
		SpringApplication.run(AbmfApplication.class, args);
		ModelInterpretationContext context;
		//context.setConfiguratorSupplier();
	}

	@Override
	public void run(String... args) {

	}

	@KafkaListener(topics = "chf-to-abmf", groupId = "abmf-consumer")
	public void listen(@Payload String message) {
		try {
			System.out.println("Kafka'dan gelen mesaj: " + message);

			ObjectMapper mapper = new ObjectMapper();
			JsonNode json = mapper.readTree(message);

			String msisdn = json.get("msisdn").asText();
			int newMinutes = json.get("new_minutes").asInt();
			int newSms = json.get("new_sms").asInt();
			int newData = json.get("new_data").asInt();

			balanceService.updateBalance(msisdn, newMinutes, newSms, newData);
		} catch (Exception e) {
			System.err.println("Mesaj işlenirken hata oluştu: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
