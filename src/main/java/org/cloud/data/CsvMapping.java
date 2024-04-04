package org.cloud.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

public class CsvMapping {


    public MappingDto MappingConversion(String mappingId, String tenantIdToken, String transactionId) throws JsonProcessingException {
        String apiUrl= "https://ig.aidtaas.com/pi-ingestion-service/api/mappingConfigs/";
        String apiUrlWithMappingId = apiUrl + mappingId;
        System.out.println("mapping rest call api + "+apiUrlWithMappingId);


        // Create HTTP headers with authorization token if needed
        HttpHeaders httpHeaders = new HttpHeaders();
        String newToken = tenantIdToken.replace("_", " ");
        httpHeaders.add("Authorization", newToken);
        System.out.println("headers : "+httpHeaders);
        HttpEntity httpEntity = new HttpEntity<>(httpHeaders);

        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<String> responseEntity;
        try {
            responseEntity = restTemplate.exchange(apiUrlWithMappingId, HttpMethod.GET, httpEntity, String.class);
            System.out.println("status code from the response : "+responseEntity.getStatusCode());
        } catch (Exception e) {
            throw new RuntimeException("Error during REST call for mapping: " + e.getMessage(), e);
        }

        // Process the response
        if (responseEntity.getStatusCode().is2xxSuccessful()) {
            // Parse the response and extract necessary information
            XmlMapper xmlMapper = new XmlMapper();
            JsonNode responseNode = xmlMapper.readTree(responseEntity.getBody());
            Map<String, Object> mappingConfigList = xmlMapper.convertValue(responseNode.get("mappingConfig"), Map.class);
            String csvUrl = (String) mappingConfigList.get("sourceEntityId");
            String schema = (String) mappingConfigList.get("destinationEntityId");
            String tableName = "t_" + schema + "_t";
            String tenantId = xmlMapper.convertValue(responseNode.get("tenantId"), String.class);
            Map<String, String> columnMappings = xmlMapper.convertValue(mappingConfigList.get("mapping"), Map.class);
            System.out.println("returning the mappingDTO");
            // Create and return MappingDto object
            return new MappingDto(csvUrl, tableName, columnMappings, tenantId, transactionId);
        } else {
            // Handle unsuccessful response
            throw new RuntimeException("Error: " + responseEntity.getStatusCode());
        }

    }

}
