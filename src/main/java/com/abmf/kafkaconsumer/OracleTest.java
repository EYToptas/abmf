package com.abmf.kafkaconsumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;

@Component
public class OracleTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @PostConstruct
    public void testConnection() {
        try {
            String result = jdbcTemplate.queryForObject("SELECT 'OK' FROM DUAL", String.class);
            System.out.println(" Oracle bağlantısı başarılı: " + result);
        } catch (Exception e) {
            System.err.println(" Oracle bağlantı hatası: " + e.getMessage());
        }
    }
}
