package com.calendar.chat.infrastructure.media.signing;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Signs Garage requests in the URL itself, with the "AWS Signature Version 4" scheme its S3 API
 * requires (the protocol keeps that name; no AWS service is involved).
 *
 * <p>Only the host is signed, and the payload is declared unsigned: a URL signed here can be
 * handed to any HTTP client, including a browser, and stays valid until it expires.
 */
public class GarageRequestSigner {

    private static final String ALGORITHM = "AWS4-HMAC-SHA256";
    private static final String SERVICE = "s3";
    private static final String TERMINATOR = "aws4_request";
    private static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
    private static final HexFormat HEX = HexFormat.of();

    private final String accessKey;
    private final String secretKey;
    private final String region;
    private final Clock clock;

    public GarageRequestSigner(String accessKey, String secretKey, String region, Clock clock) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.clock = clock;
    }

    /**
     * A URL for {@code method} on {@code path} (already in "/bucket/key" form) at {@code endpoint},
     * valid for {@code ttl}. {@code extraQuery} is signed with the rest, so it cannot be altered.
     */
    public URI presign(String method, URI endpoint, String path, Duration ttl, Map<String, String> extraQuery) {
        String timestamp = TIMESTAMP.format(clock.instant());
        String scope = DAY.format(clock.instant()) + "/" + region + "/" + SERVICE + "/" + TERMINATOR;
        String host = endpoint.getPort() == -1 ? endpoint.getHost() : endpoint.getHost() + ":" + endpoint.getPort();
        String canonicalPath = encodePath(path);

        Map<String, String> query = new TreeMap<>(extraQuery);
        query.put("X-Amz-Algorithm", ALGORITHM);
        query.put("X-Amz-Credential", accessKey + "/" + scope);
        query.put("X-Amz-Date", timestamp);
        query.put("X-Amz-Expires", Long.toString(ttl.toSeconds()));
        query.put("X-Amz-SignedHeaders", "host");
        String canonicalQuery = query.entrySet().stream()
                .map(entry -> encode(entry.getKey()) + "=" + encode(entry.getValue()))
                .collect(Collectors.joining("&"));

        String canonicalRequest = String.join("\n",
                method, canonicalPath, canonicalQuery, "host:" + host, "", "host", UNSIGNED_PAYLOAD);
        String stringToSign = String.join("\n", ALGORITHM, timestamp, scope, HEX.formatHex(sha256(canonicalRequest)));
        String signature = HEX.formatHex(hmac(signingKey(DAY.format(clock.instant())), stringToSign));

        return URI.create(endpoint.getScheme() + "://" + host + canonicalPath
                + "?" + canonicalQuery + "&X-Amz-Signature=" + signature);
    }

    /** Derived per day, region and service, so the secret itself never signs anything. */
    private byte[] signingKey(String day) {
        byte[] key = hmac(("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8), day);
        key = hmac(key, region);
        key = hmac(key, SERVICE);
        return hmac(key, TERMINATOR);
    }

    private static String encodePath(String path) {
        return Arrays.stream(path.split("/", -1))
                .map(GarageRequestSigner::encode)
                .collect(Collectors.joining("/"));
    }

    /** RFC 3986: only unreserved characters stay as they are, as the signature scheme requires. */
    static String encode(String value) {
        StringBuilder encoded = new StringBuilder();
        for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
            char c = (char) (b & 0xFF);
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                    || c == '-' || c == '_' || c == '.' || c == '~') {
                encoded.append(c);
            } else {
                encoded.append('%').append(HEX.withUpperCase().toHexDigits(b));
            }
        }
        return encoded.toString();
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SHA-256 is missing from the JDK", e);
        }
    }

    private static byte[] hmac(byte[] key, String value) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is missing from the JDK", e);
        }
    }
}
