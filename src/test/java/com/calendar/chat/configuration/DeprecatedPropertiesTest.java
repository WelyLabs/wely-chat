package com.calendar.chat.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fails the build when a property file uses a key Spring Boot no longer binds.
 *
 * <p>Written after {@code spring.data.mongodb.uri} cost this service a production outage.
 * Spring Boot 4 moved the MongoDB connection settings to {@code spring.mongodb.*} and marked
 * the old names deprecated at <em>error</em> level, which means they are parsed, ignored, and
 * never mentioned. The service started cleanly, reported itself healthy — the readiness probe
 * excludes datastore indicators on purpose — and connected to {@code localhost:27017}, the
 * default for a URI nobody set. Nothing in the logs said why.
 *
 * <p>No integration test could catch it either: Testcontainers sets the URI through
 * {@code @ServiceConnection}, so the tests bypass the very file that was wrong.
 *
 * <p>The deprecation metadata ships inside the Spring jars, so this reads what the version on
 * the classpath actually says rather than a list someone has to remember to update. Boot 4.0.1
 * carries 377 such keys; the next upgrade will carry different ones.
 */
class DeprecatedPropertiesTest {

    private static final String[] FILES = {
            "application.properties",
            "application-dev.properties",
            "application-test.properties",
    };

    @Test
    @DisplayName("no property file uses a key Spring Boot silently ignores")
    void properties_shouldNotUseKeysDeprecatedAtErrorLevel() throws IOException {
        Map<String, String> removed = errorLevelDeprecations();
        assertThat(removed)
                .as("deprecation metadata should be on the test classpath")
                .isNotEmpty();

        List<String> offences = new ArrayList<>();
        for (String file : FILES) {
            for (String key : keysIn(file)) {
                String replacement = removed.get(key);
                if (replacement != null) {
                    offences.add("%s: %s -> %s".formatted(file, key, replacement));
                }
            }
        }

        assertThat(offences)
                .as("properties Spring Boot parses, ignores, and says nothing about")
                .isEmpty();
    }

    /** Every key the Spring jars on the classpath declare as deprecated at error level. */
    private Map<String, String> errorLevelDeprecations() throws IOException {
        Map<String, String> result = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        Resource[] metadata = new PathMatchingResourcePatternResolver()
                .getResources("classpath*:META-INF/spring-configuration-metadata.json");

        for (Resource resource : metadata) {
            JsonNode root;
            try (InputStream in = resource.getInputStream()) {
                root = mapper.readTree(in);
            }
            for (JsonNode property : root.path("properties")) {
                JsonNode deprecation = property.path("deprecation");
                if ("error".equals(deprecation.path("level").asText())) {
                    result.put(property.path("name").asText(),
                            deprecation.path("replacement").asText("(no replacement)"));
                }
            }
        }
        return result;
    }

    private Iterable<String> keysIn(String file) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(file)) {
            if (in == null) {
                return List.of();
            }
            properties.load(in);
        }
        return properties.stringPropertyNames();
    }
}
