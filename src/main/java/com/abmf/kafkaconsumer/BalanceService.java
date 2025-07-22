package com.abmf.kafkaconsumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlOutParameter;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import java.sql.Types;
import oracle.jdbc.OracleTypes;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import jakarta.annotation.PostConstruct;
@Service
public class BalanceService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private SimpleJdbcCall updateBalanceProc;
    private SimpleJdbcCall getBalanceProc;

    @PostConstruct
    public void init() {
        this.updateBalanceProc = new SimpleJdbcCall(jdbcTemplate)
                .withProcedureName("update_balance_all")
                .withoutProcedureColumnMetaDataAccess() //
                .declareParameters(
                        new SqlParameter("p_msisdn", Types.VARCHAR),
                        new SqlParameter("p_new_minutes", Types.INTEGER),
                        new SqlParameter("p_new_sms", Types.INTEGER),
                        new SqlParameter("p_new_data", Types.INTEGER),
                        new SqlOutParameter("o_status_code", Types.INTEGER)
                );


        this.getBalanceProc = new SimpleJdbcCall(jdbcTemplate)
                .withProcedureName("get_balance")
                .withoutProcedureColumnMetaDataAccess()
                .declareParameters(
                        new SqlParameter("p_msisdn", Types.VARCHAR),
                        new SqlOutParameter("o_balance", OracleTypes.CURSOR, new ColumnMapRowMapper()),
                        new SqlOutParameter("o_status_code", Types.INTEGER)
                );
    }

    public void getBalance(String msisdn) {
        SimpleJdbcCall jdbcCall = new SimpleJdbcCall(jdbcTemplate)
                .withProcedureName("get_balance")
                .withoutProcedureColumnMetaDataAccess()
                .declareParameters(
                        new SqlParameter("p_msisdn", Types.VARCHAR),
                        new SqlOutParameter("o_balance", OracleTypes.CURSOR, new ColumnMapRowMapper()),
                        new SqlOutParameter("o_status_code", Types.INTEGER)
                );

        Map<String, Object> inParams = new HashMap<>();
        inParams.put("p_msisdn", msisdn);

        Map<String, Object> result = jdbcCall.execute(inParams);

        Integer statusCode = (Integer) result.get("o_status_code");

        if (statusCode != null && statusCode == 200) {
            List<Map<String, Object>> balanceList = (List<Map<String, Object>>) result.get("o_balance");
            for (Map<String, Object> row : balanceList) {
                System.out.println("Kalan Kullanım Bilgisi: " + row);
            }
        } else {
            System.err.println("Prosedür çalıştırılamadı, status_code: " + statusCode);
        }
    }



    public void updateBalance(String msisdn, int minutes, int sms, int data) {
        Map<String, Object> inParams = new HashMap<>();
        inParams.put("p_msisdn", msisdn);
        inParams.put("p_new_minutes", minutes);
        inParams.put("p_new_sms", sms);
        inParams.put("p_new_data", data);

        Map<String, Object> result = updateBalanceProc.execute(inParams);

        System.out.println("✅ update_balance_all sonucu: " + result);
    }
}
