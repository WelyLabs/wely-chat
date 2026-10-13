package com.calendar.chat.infrastructure.media.adapters;

import com.calendar.chat.configuration.MediaClientConfig;
import com.calendar.chat.infrastructure.media.signing.GarageRequestSigner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The storage against a real Garage: what the bucket accepts, what a signed URL opens, and what it
 * refuses. These are properties of Garage itself, and of whether it accepts our signatures, which
 * a stubbed exchange cannot settle.
 *
 * <p>Opt-in: CI sets {@code CI=true}, locally pass {@code -Dintegration.tests=true}.
 */
@Testcontainers
@EnabledIf("containersRequested")
class GaragePhotoStorageAdapterIntegrationTest {

    private static final String ACCESS_KEY = "GK0123456789abcdef01234567";
    private static final String SECRET_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static final String BUCKET = "wely-chat-media";

    @Container
    static final GenericContainer<?> GARAGE = new GenericContainer<>("dxflrs/garage:v2.4.1")
            .withCopyFileToContainer(MountableFile.forClasspathResource("garage/garage.toml"), "/etc/garage.toml")
            .withExposedPorts(3900, 3903)
            .waitingFor(Wait.forHttp("/health").forPort(3903).forStatusCodeMatching(code -> code < 600));

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static GaragePhotoStorageAdapter storage;

    static boolean containersRequested() {
        return System.getenv("CI") != null || Boolean.getBoolean("integration.tests");
    }

    /** The same steps as the gitops setup Job: layout, bucket, imported key, permissions. */
    @BeforeAll
    static void setUpGarage() throws Exception {
        String node = admin("GetClusterStatus", null).replaceAll("(?s).*?\"id\"\\s*:\\s*\"([0-9a-f]+)\".*", "$1");
        admin("UpdateClusterLayout", "{\"roles\":[{\"id\":\"" + node + "\",\"zone\":\"test\",\"capacity\":1000000000,\"tags\":[]}]}");
        admin("ApplyClusterLayout", "{\"version\":1}");
        String bucket = admin("CreateBucket", "{\"globalAlias\":\"" + BUCKET + "\"}")
                .replaceAll("(?s).*?\"id\"\\s*:\\s*\"([0-9a-f]+)\".*", "$1");
        admin("ImportKey", "{\"accessKeyId\":\"" + ACCESS_KEY + "\",\"secretAccessKey\":\"" + SECRET_KEY + "\",\"name\":\"wely-users\"}");
        admin("AllowBucketKey", "{\"bucketId\":\"" + bucket + "\",\"accessKeyId\":\"" + ACCESS_KEY
                + "\",\"permissions\":{\"read\":true,\"write\":true,\"owner\":false}}");

        URI endpoint = URI.create("http://" + GARAGE.getHost() + ":" + GARAGE.getMappedPort(3900));
        storage = new GaragePhotoStorageAdapter(new MediaClientConfig().mediaWebClient(),
                new GarageRequestSigner(ACCESS_KEY, SECRET_KEY, "garage", Clock.systemUTC()), endpoint, endpoint, BUCKET);
    }

    private static String admin(String endpoint, String body) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(
                        "http://" + GARAGE.getHost() + ":" + GARAGE.getMappedPort(3903) + "/v2/" + endpoint))
                .header("Authorization", "Bearer test-admin-token")
                .header("Content-Type", "application/json");
        request = body == null ? request.GET() : request.POST(HttpRequest.BodyPublishers.ofString(body));
        HttpResponse<String> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(endpoint + ": " + response.body()).isEqualTo(200);
        return response.body();
    }

    private static HttpResponse<byte[]> get(String url) throws IOException, InterruptedException {
        return HTTP.send(HttpRequest.newBuilder(URI.create(url)).build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    @Test
    @DisplayName("a stored picture opens with its signed URL, as a privately cached JPEG")
    void signedUrl_shouldOpenTheStoredPicture() throws Exception {
        storage.put("avatars/u1/a.jpg", new byte[] {(byte) 0xFF, (byte) 0xD8, 1}, "image/jpeg").block();

        HttpResponse<byte[]> response = get(storage.signedUrl("avatars/u1/a.jpg", Duration.ofMinutes(5)));

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).containsExactly(0xFF, 0xD8, 1);
        assertThat(response.headers().firstValue("content-type")).hasValue("image/jpeg");
        assertThat(response.headers().firstValue("cache-control")).hasValue("private, max-age=3600");
    }

    @Test
    @DisplayName("the bucket is private: no signature, or a forged one, opens nothing")
    void get_shouldRefuseAnUnsignedOrForgedUrl() throws Exception {
        storage.put("avatars/u2/a.jpg", new byte[] {1}, "image/jpeg").block();
        String signed = storage.signedUrl("avatars/u2/a.jpg", Duration.ofMinutes(5));
        String forged = signed.replaceAll("X-Amz-Signature=[0-9a-f]{8}", "X-Amz-Signature=00000000");

        assertThat(get(signed.substring(0, signed.indexOf('?'))).statusCode()).isEqualTo(403);
        assertThat(get(forged).statusCode()).isEqualTo(403);
    }

    @Test
    void signedUrl_shouldStopWorkingOnceExpired() throws Exception {
        storage.put("avatars/u3/a.jpg", new byte[] {1}, "image/jpeg").block();
        String shortLived = storage.signedUrl("avatars/u3/a.jpg", Duration.ofSeconds(1));

        Thread.sleep(2_500);

        assertThat(get(shortLived).statusCode()).isBetween(400, 403);
    }

    @Test
    void delete_shouldRemoveThePicture_andToleratesAMissingOne() throws Exception {
        storage.put("avatars/u4/a.jpg", new byte[] {1}, "image/jpeg").block();
        String signed = storage.signedUrl("avatars/u4/a.jpg", Duration.ofMinutes(5));

        storage.delete("avatars/u4/a.jpg").block();
        storage.delete("avatars/u4/a.jpg").block();

        assertThat(get(signed).statusCode()).isEqualTo(404);
    }
}
