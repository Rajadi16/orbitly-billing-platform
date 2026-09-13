package com.orbitly.auth;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "orbitly.jwt.secret=test-secret-key-must-be-at-least-32-chars-long",
                "orbitly.stripe.api-key=sk_test_dummy",
                "orbitly.stripe.webhook-secret=whsec_dummy",
                "spring.kafka.producer.bootstrap-servers=",
                "spring.kafka.consumer.bootstrap-servers=",
                "spring.kafka.admin.bootstrap-servers="
        }
)
@AutoConfigureMockMvc
@Testcontainers
class AuthIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    private static final String REGISTER_URL = "/api/v1/auth/register";
    private static final String LOGIN_URL    = "/api/v1/auth/login";

    // ── Register ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("POST /register with valid body → 200 + JWT token returned")
    void register_validRequest_returnsToken() throws Exception {
        mockMvc.perform(post(REGISTER_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"email":"alice@orbitly.com","password":"password123"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token", not(emptyString())))
                .andExpect(jsonPath("$.email", is("alice@orbitly.com")))
                .andExpect(jsonPath("$.role", is("USER")));
    }

    @Test
    @DisplayName("POST /register with duplicate email → 409 Conflict")
    void register_duplicateEmail_returns409() throws Exception {
        String body = """
                {"email":"bob@orbitly.com","password":"password123"}
                """;
        // First registration
        mockMvc.perform(post(REGISTER_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk());

        // Duplicate
        mockMvc.perform(post(REGISTER_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isConflict());
    }

    @Test
    @DisplayName("POST /register with invalid email → 400 Bad Request")
    void register_invalidEmail_returns400() throws Exception {
        mockMvc.perform(post(REGISTER_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"email":"not-an-email","password":"password123"}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title", is("Validation failed")));
    }

    @Test
    @DisplayName("POST /register with short password → 400 Bad Request")
    void register_shortPassword_returns400() throws Exception {
        mockMvc.perform(post(REGISTER_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"email":"carol@orbitly.com","password":"short"}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title", is("Validation failed")));
    }

    // ── Login ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("POST /login with correct credentials → 200 + JWT token")
    void login_validCredentials_returnsToken() throws Exception {
        // Register first
        mockMvc.perform(post(REGISTER_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"email":"dave@orbitly.com","password":"password123"}
                                """))
                .andExpect(status().isOk());

        // Then login
        mockMvc.perform(post(LOGIN_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"email":"dave@orbitly.com","password":"password123"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token", not(emptyString())))
                .andExpect(jsonPath("$.email", is("dave@orbitly.com")));
    }

    @Test
    @DisplayName("POST /login with wrong password → 401 Unauthorized")
    void login_wrongPassword_returns401() throws Exception {
        // Register first
        mockMvc.perform(post(REGISTER_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"email":"eve@orbitly.com","password":"password123"}
                                """))
                .andExpect(status().isOk());

        // Wrong password
        mockMvc.perform(post(LOGIN_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"email":"eve@orbitly.com","password":"wrongpassword"}
                                """))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("POST /login with non-existent user → 401 Unauthorized")
    void login_nonExistentUser_returns401() throws Exception {
        mockMvc.perform(post(LOGIN_URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"email":"ghost@orbitly.com","password":"password123"}
                                """))
                .andExpect(status().isUnauthorized());
    }
}
