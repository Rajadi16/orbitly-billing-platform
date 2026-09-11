package com.orbitly.invoice;

import com.orbitly.invoice.dto.CreateInvoiceRequest;
import com.orbitly.invoice.dto.InvoiceResponse;
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
public class InvoiceController {

    private final InvoiceService invoiceService;

    /** Create a new DRAFT invoice for the authenticated user. */
    @PostMapping
    public ResponseEntity<InvoiceResponse> create(
            @AuthenticationPrincipal UserDetails caller,
            @Valid @RequestBody CreateInvoiceRequest request) {

        InvoiceResponse response = invoiceService.create(caller.getUsername(), request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /** List all invoices belonging to the authenticated user (paginated). */
    @GetMapping
    public ResponseEntity<Page<InvoiceResponse>> list(
            @AuthenticationPrincipal UserDetails caller,
            @PageableDefault(size = 20, sort = "createdAt") Pageable pageable) {

        return ResponseEntity.ok(invoiceService.list(caller.getUsername(), pageable));
    }

    /** Get a single invoice — returns 403 if invoice belongs to a different user. */
    @GetMapping("/{id}")
    public ResponseEntity<InvoiceResponse> get(
            @AuthenticationPrincipal UserDetails caller,
            @PathVariable UUID id) {

        return ResponseEntity.ok(invoiceService.get(caller.getUsername(), id));
    }

    /** Transition invoice DRAFT → PENDING and stub-publish an invoice.created event. */
    @PostMapping("/{id}/send")
    public ResponseEntity<InvoiceResponse> send(
            @AuthenticationPrincipal UserDetails caller,
            @PathVariable UUID id) {

        return ResponseEntity.ok(invoiceService.send(caller.getUsername(), id));
    }
}
