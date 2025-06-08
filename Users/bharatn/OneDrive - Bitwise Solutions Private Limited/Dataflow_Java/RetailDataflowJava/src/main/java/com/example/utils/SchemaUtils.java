package com.example.utils;

import com.example.models.Sale;

public class SchemaUtils {
    public static Sale parseSale(String line) {
        String[] fields = line.split(",");
        return new Sale(
            fields[0], fields[1], fields[2],
            Integer.parseInt(fields[3]), Double.parseDouble(fields[4])
        );
    }

    public static String formatSale(Sale sale) {
        return String.join(",",
            sale.getSaleId(),
            sale.getProductId(),
            sale.getStoreId(),
            String.valueOf(sale.getQuantity()),
            String.valueOf(sale.getTotalAmount())
        );
    }
}
