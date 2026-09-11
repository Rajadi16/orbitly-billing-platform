package com.orbitly.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI orbitlyOpenAPI() {
        final String schemeName = "bearerAuth";

        return new OpenAPI()
                .info(new Info()
                        .title("Orbitly Billing API")
                        .description("""
                                Event-driven SaaS billing backend.
                                Authenticate via POST /api/v1/auth/login, then use the returned JWT as a Bearer token.
                                Stripe webhooks are public (signature-verified by the server).
                                """)
                        .version("0.1.0")
                        .contact(new Contact()
                                .name("Orbitly")
                                .url("https://github.com/Rajadi16/orbitly-billing-platform")))
                .addSecurityItem(new SecurityRequirement().addList(schemeName))
                .components(new Components()
                        .addSecuritySchemes(schemeName, new SecurityScheme()
                                .name(schemeName)
                                .type(SecurityScheme.Type.HTTP)
                                .scheme("bearer")
                                .bearerFormat("JWT")));
    }
}
