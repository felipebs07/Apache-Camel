package com.felipebs.apachecamel.integration;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class RouteAC extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        from("timer:weatherPoll?period=600000") // 10 minutos = 600.000 ms
                .routeId("weather-api-to-kafka")

                // 1. Chamada à API do OpenWeatherMap
                .setHeader("CamelHttpMethod", constant("GET"))
                .to("http://api.openweathermap.org/data/2.5/weather?lat=44.34&lon=10.99&appid=?")

                // 2. Processamento dos dados (extrair temperatura)
                .process(exchange -> {
                    String jsonResponse = exchange.getIn().getBody(String.class);
                    double temperatura = extractTemperature(jsonResponse);
                    exchange.getMessage().setBody(temperatura);
                })

                // 3. Tratamento de erros
                .onException(Exception.class)
                    .handled(true)
                    .log("Erro ao processar dados da API: ${exception.message}")
                    .to("kafka:weather-errors?brokers=localhost:9092")
                .end()

                .log("Temperatura de São Paulo: ${body}°C")
                .to("kafka:weather-data?brokers=localhost:9092");
    }

    private double extractTemperature(String json) {
        return Double.parseDouble(json.split("\"temp\":")[1].split(",")[0]);
    }
}
