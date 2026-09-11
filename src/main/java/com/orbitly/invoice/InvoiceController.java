package com.orbitly.invoice;

import com.orbitly.invoice.dto.CreateInvoiceRequest;
import com.orbitly.invoice.dto.InvoiceResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/invoices")
@RequiredArgsConstructor
@Tag(name = "Invoices", description = "Endpoints for managing invoices (requires JWT)")
public class InvoiceController {

    private final InvoiceService invoiceService;

    @Operation(summary = "Create DRAFT invoice", description = "Creates a new DRAFT invoice for the authenticated user.")
    @PostMapping
    public ResponseEntity<InvoiceResponse> create(
            @AuthenticationPrincipal UserDetails caller,
            @Valid @RequestBody CreateInvoiceRequest request) {

        InvoiceResponse response = invoiceService.create(caller.getUsername(), request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @Operation(summary = "List invoices", description = "Returns a paginated list of the authenticated user's invoices.")
    @GetMapping
    public ResponseEntity<Page<InvoiceResponse>> list(
            @AuthenticationPrincipal UserDetails caller,
            @PageableDefault(size = 20, sort = "createdAt") Pageable pageable) {

        return ResponseEntity.ok(invoiceService.list(caller.getUsername(), pageable));
    }

    @Operation(summary = "Get single invoice", description = "Returns an invoice by ID. 403 if it belongs to another user.")
    @GetMapping("/{id}")
    public ResponseEntity<InvoiceResponse> get(
            @AuthenticationPrincipal UserDetails caller,
            @PathVariable UUID id) {

        return ResponseEntity.ok(invoiceService.get(caller.getUsername(), id));
    }

    @Operation(summary = "Send invoice", description = "Transitions a DRAFT invoice to PENDING and publishes an event to Kafka.")
    @PostMapping("/{id}/send")
    public ResponseEntity<InvoiceResponse> send(
            @AuthenticationPrincipal UserDetails caller,
            @PathVariable UUID id) {

        return ResponseEntity.ok(invoiceService.send(caller.getUsername(), id));
    }
}
