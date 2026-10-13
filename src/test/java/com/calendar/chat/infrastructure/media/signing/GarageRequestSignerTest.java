package com.calendar.chat.infrastructure.media.signing;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GarageRequestSignerTest {

    private static final Clock FIXED = Clock.fixed(Instant.parse("2013-05-24T00:00:00Z"), ZoneOffset.UTC);

    @Test
    @DisplayName("matches the worked example of the signature specification, byte for byte")
    void presign_shouldMatchTheSpecificationExample() {
        // The query-string example from the Signature Version 4 specification, with its published
        // credentials and result: a signer that agrees with it agrees with every S3 server.
        GarageRequestSigner signer = new GarageRequestSigner("AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", FIXED);

        URI url = signer.presign("GET", URI.create("https://examplebucket.s3.amazonaws.com"), "/test.txt",
                Duration.ofHours(24), Map.of());

        assertThat(url.getRawQuery())
                .endsWith("X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404");
    }

    @Test
    @DisplayName("a URL names the endpoint's host and port, the path, the expiry and the extra parameters")
    void presign_shouldCarryTheEndpointPathExpiryAndExtraQuery() {
        GarageRequestSigner signer = new GarageRequestSigner("GK000000000000000000000000", "secret", "garage", FIXED);

        URI url = signer.presign("GET", URI.create("http://localhost:3900"), "/wely-avatars/avatars/u1/a.jpg",
                Duration.ofMinutes(10), Map.of("response-cache-control", "private, max-age=3600"));

        assertThat(url.getScheme()).isEqualTo("http");
        assertThat(url.getAuthority()).isEqualTo("localhost:3900");
        assertThat(url.getRawPath()).isEqualTo("/wely-avatars/avatars/u1/a.jpg");
        assertThat(url.getRawQuery())
                .contains("X-Amz-Credential=GK000000000000000000000000%2F20130524%2Fgarage%2Fs3%2Faws4_request")
                .contains("X-Amz-Expires=600")
                .contains("response-cache-control=private%2C%20max-age%3D3600");
    }

    @Test
    @DisplayName("the method is part of the signature: a read URL cannot be replayed as a write")
    void presign_shouldSignTheMethod() {
        GarageRequestSigner signer = new GarageRequestSigner("GK000000000000000000000000", "secret", "garage", FIXED);
        URI endpoint = URI.create("http://localhost:3900");

        URI read = signer.presign("GET", endpoint, "/b/k", Duration.ofMinutes(1), Map.of());
        URI write = signer.presign("PUT", endpoint, "/b/k", Duration.ofMinutes(1), Map.of());

        assertThat(read).isNotEqualTo(write);
    }

    @Test
    void encode_shouldKeepOnlyUnreservedCharacters() {
        assertThat(GarageRequestSigner.encode("a-Z_0.9~")).isEqualTo("a-Z_0.9~");
        assertThat(GarageRequestSigner.encode("a b/c+é")).isEqualTo("a%20b%2Fc%2B%C3%A9");
    }
}
