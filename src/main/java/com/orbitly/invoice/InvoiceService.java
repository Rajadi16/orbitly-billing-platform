package com.orbitly.invoice;

import com.orbitly.invoice.dto.CreateInvoiceRequest;
import com.orbitly.invoice.dto.InvoiceResponse;
import com.orbitly.user.User;
import com.orbitly.user.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class InvoiceService {

    private final InvoiceRepository    invoiceRepository;
    private final InvoiceEventProducer invoiceEventProducer;
    private final UserRepository    userRepository;

    // ── Create DRAFT invoice ─────────────────────────────────────────────────

    @Transactional
    public InvoiceResponse create(String callerEmail, CreateInvoiceRequest req) {
        User user = loadUser(callerEmail);

        Invoice invoice = Invoice.builder()
                .user(user)
                .amountCents(req.amountCents())
                .currency(req.currency().toUpperCase())
                .status(InvoiceStatus.DRAFT)
                .build();

        return InvoiceResponse.from(invoiceRepository.save(invoice));
    }

    // ── List invoices (current user only, paginated) ─────────────────────────

    @Transactional(readOnly = true)
    public Page<InvoiceResponse> list(String callerEmail, Pageable pageable) {
        User user = loadUser(callerEmail);
        return invoiceRepository.findByUser_Id(user.getId(), pageable)
                .map(InvoiceResponse::from);
    }

    // ── Get single invoice (403 if not owned by caller) ──────────────────────

    @Transactional(readOnly = true)
    public InvoiceResponse get(String callerEmail, UUID invoiceId) {
        Invoice invoice = loadAndVerifyOwnership(callerEmail, invoiceId);
        return InvoiceResponse.from(invoice);
    }

    // ── Send: DRAFT → PENDING (stubs Kafka — wired in Step 4) ───────────────

    @Transactional
    public InvoiceResponse send(String callerEmail, UUID invoiceId) {
        Invoice invoice = loadAndVerifyOwnership(callerEmail, invoiceId);

        if (invoice.getStatus() != InvoiceStatus.DRAFT) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Only DRAFT invoices can be sent (current status: " + invoice.getStatus() + ")");
        }

        invoice.setStatus(InvoiceStatus.PENDING);
        invoiceRepository.save(invoice);

        invoiceEventProducer.publishInvoiceCreated(
                invoice.getId(),
                invoice.getAmountCents(),
                invoice.getCurrency()
        );
        log.info("Invoice [id={}] transitioned DRAFT → PENDING, invoice.created published to Kafka",
                invoice.getId());

        return InvoiceResponse.from(invoice);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private User loadUser(String email) {
        return userRepository.findByEmail(email)
                .orElseThrow(() -> new UsernameNotFoundException("User not found: " + email));
    }

    private Invoice loadAndVerifyOwnership(String callerEmail, UUID invoiceId) {
        Invoice invoice = invoiceRepository.findById(invoiceId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Invoice not found: " + invoiceId));

        if (!invoice.getUser().getUsername().equals(callerEmail)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN,
                    "Access denied to invoice: " + invoiceId);
        }

        return invoice;
    }
}
