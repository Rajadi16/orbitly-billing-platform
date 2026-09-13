package com.orbitly.invoice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbitly.invoice.dto.CreateInvoiceRequest;
import com.orbitly.invoice.dto.InvoiceResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.server.ResponseStatusException;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(
        controllers = InvoiceController.class,
        excludeAutoConfiguration = org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration.class,
        properties = {
                "orbitly.jwt.secret=test-secret-key-must-be-at-least-32-chars-long",
                "orbitly.jwt.expiration-ms=3600000"
        }
)
class InvoiceControllerTest {

    @Autowired MockMvc     mockMvc;
    @Autowired ObjectMapper objectMapper;

    @MockBean InvoiceService invoiceService;

    private static final String USER_EMAIL = "alice@orbitly.com";
    private static final UUID   INVOICE_ID = UUID.randomUUID();

    private InvoiceResponse sampleResponse() {
        return new InvoiceResponse(
                INVOICE_ID, UUID.randomUUID(), 5000L, "USD",
                "DRAFT", null,
                OffsetDateTime.now(), OffsetDateTime.now()
        );
    }

    // ── POST /api/v1/invoices ────────────────────────────────────────────────

    @Test
    @WithMockUser(username = USER_EMAIL)
    @DisplayName("POST /invoices — valid body → 201 Created")
    void createInvoice_validRequest_returns201() throws Exception {
        when(invoiceService.create(eq(USER_EMAIL), any())).thenReturn(sampleResponse());

        mockMvc.perform(post("/api/v1/invoices")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"amountCents":5000,"currency":"USD"}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").isNotEmpty())
                .andExpect(jsonPath("$.status").value("DRAFT"));
    }

    @Test
    @WithMockUser(username = USER_EMAIL)
    @DisplayName("POST /invoices — missing currency → 400")
    void createInvoice_missingCurrency_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/invoices")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"amountCents":5000}
                                """))
                .andExpect(status().isBadRequest());
    }

    // ── GET /api/v1/invoices ─────────────────────────────────────────────────

    @Test
    @WithMockUser(username = USER_EMAIL)
    @DisplayName("GET /invoices — returns only caller's invoices")
    void listInvoices_returnsOnlyOwnInvoices() throws Exception {
        var page = new PageImpl<>(List.of(sampleResponse()), PageRequest.of(0, 20), 1);
        when(invoiceService.list(eq(USER_EMAIL), any())).thenReturn(page);

        mockMvc.perform(get("/api/v1/invoices"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content").isArray())
                .andExpect(jsonPath("$.content[0].status").value("DRAFT"))
                .andExpect(jsonPath("$.totalElements").value(1));
    }

    // ── GET /api/v1/invoices/{id} ────────────────────────────────────────────

    @Test
    @WithMockUser(username = USER_EMAIL)
    @DisplayName("GET /invoices/{id} — owned invoice → 200")
    void getInvoice_ownedByUser_returns200() throws Exception {
        when(invoiceService.get(eq(USER_EMAIL), eq(INVOICE_ID))).thenReturn(sampleResponse());

        mockMvc.perform(get("/api/v1/invoices/" + INVOICE_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(INVOICE_ID.toString()));
    }

    @Test
    @WithMockUser(username = USER_EMAIL)
    @DisplayName("GET /invoices/{id} — not owner → 403 Forbidden")
    void getInvoice_notOwner_returns403() throws Exception {
        when(invoiceService.get(eq(USER_EMAIL), eq(INVOICE_ID)))
                .thenThrow(new ResponseStatusException(HttpStatus.FORBIDDEN, "Access denied"));

        mockMvc.perform(get("/api/v1/invoices/" + INVOICE_ID))
                .andExpect(status().isForbidden());
    }

    // ── POST /api/v1/invoices/{id}/send ──────────────────────────────────────

    @Test
    @WithMockUser(username = USER_EMAIL)
    @DisplayName("POST /invoices/{id}/send — DRAFT invoice → status becomes PENDING")
    void sendInvoice_draft_becomesPending() throws Exception {
        InvoiceResponse pending = new InvoiceResponse(
                INVOICE_ID, UUID.randomUUID(), 5000L, "USD",
                "PENDING", null,
                OffsetDateTime.now(), OffsetDateTime.now()
        );
        when(invoiceService.send(eq(USER_EMAIL), eq(INVOICE_ID))).thenReturn(pending);

        mockMvc.perform(post("/api/v1/invoices/" + INVOICE_ID + "/send")
                        .with(csrf()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    @WithMockUser(username = USER_EMAIL)
    @DisplayName("POST /invoices/{id}/send — non-DRAFT invoice → 400")
    void sendInvoice_notDraft_returns400() throws Exception {
        when(invoiceService.send(eq(USER_EMAIL), eq(INVOICE_ID)))
                .thenThrow(new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Only DRAFT invoices can be sent"));

        mockMvc.perform(post("/api/v1/invoices/" + INVOICE_ID + "/send")
                        .with(csrf()))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("GET /invoices — unauthenticated → 401")
    void listInvoices_noAuth_returns401() throws Exception {
        mockMvc.perform(get("/api/v1/invoices"))
                .andExpect(status().isUnauthorized());
    }
}
