package com.orbitly.auth.dto;

/**
 * Returned to the client after successful register or login.
 */
public record AuthResponse(
        String token,
        String email,
        String role
) {}
