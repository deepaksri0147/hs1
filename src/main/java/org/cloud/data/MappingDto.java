package org.cloud.data;

import lombok.*;

import java.util.Map;


@Data
@Getter
@Setter
@Builder
public class MappingDto {

    private String csvUrl;

    private String tableName;

    private Map<String,String> columnMapping;

    private String tenantId;

    private String transactionId;

}
