package com.calendar.chat.infrastructure.media.adapters;

import com.calendar.chat.domain.models.NormalizedPhoto;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class ImageIoPhotoNormalizerAdapterTest {

    private final ImageIoPhotoNormalizerAdapter normalizer = new ImageIoPhotoNormalizerAdapter();

    private static byte[] image(int width, int height, String format) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ImageIO.write(new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB), format, bytes);
        return bytes.toByteArray();
    }

    private static boolean invalid(Throwable error) {
        return error instanceof ChatException chat && chat.getErrorCode() == ChatErrorCode.PHOTO_INVALID;
    }

    @Test
    @DisplayName("a landscape photo keeps its proportions, its longest side brought down to 1600")
    void normalize_shouldBoundTheLongestSide_keepingProportions() throws IOException {
        StepVerifier.create(normalizer.normalize(image(4000, 3000, "png")))
                .assertNext(photo -> {
                    assertThat(photo.width()).isEqualTo(1600);
                    assertThat(photo.height()).isEqualTo(1200);
                    assertThat(photo.contentType()).isEqualTo("image/jpeg");
                    assertThat(decodedSize(photo)).isEqualTo("1600x1200");
                })
                .verifyComplete();
    }

    @Test
    void normalize_shouldBoundAPortraitByItsHeight() throws IOException {
        StepVerifier.create(normalizer.normalize(image(900, 2400, "jpg")))
                .assertNext(photo -> assertThat(decodedSize(photo)).isEqualTo("600x1600"))
                .verifyComplete();
    }

    @Test
    void normalize_shouldNeverEnlargeASmallPhoto() throws IOException {
        StepVerifier.create(normalizer.normalize(image(320, 240, "png")))
                .assertNext(photo -> assertThat(decodedSize(photo)).isEqualTo("320x240"))
                .verifyComplete();
    }

    @Test
    void normalize_shouldRefuseWhatIsNotAnImage() {
        StepVerifier.create(normalizer.normalize("not a photo".getBytes(StandardCharsets.UTF_8)))
                .expectErrorMatches(ImageIoPhotoNormalizerAdapterTest::invalid)
                .verify();
    }

    @Test
    @DisplayName("a photo too big to hold in memory is refused from its header, before its pixels are decoded")
    void normalize_shouldRefuseAHugeImage() throws IOException {
        StepVerifier.create(normalizer.normalize(image(ImageIoPhotoNormalizerAdapter.MAX_SOURCE_SIDE + 1, 10, "png")))
                .expectErrorMatches(ImageIoPhotoNormalizerAdapterTest::invalid)
                .verify();
        // 7000 x 7000 is under the side limit but 49 megapixels.
        StepVerifier.create(normalizer.normalize(image(7000, 7000, "png")))
                .expectErrorMatches(ImageIoPhotoNormalizerAdapterTest::invalid)
                .verify();
    }

    @Test
    @DisplayName("what is stored is a fresh JPEG: no metadata from the original survives")
    void normalize_shouldDropTheOriginalsMetadata() throws IOException {
        byte[] jpeg = image(200, 100, "jpg");
        // An APP1 (EXIF) segment right after the SOI marker, as phones write their GPS position.
        byte[] exif = "Exif\0\0GPS-LATITUDE-45.76".getBytes(StandardCharsets.ISO_8859_1);
        ByteArrayOutputStream withExif = new ByteArrayOutputStream();
        withExif.write(jpeg, 0, 2);
        withExif.write(0xFF);
        withExif.write(0xE1);
        withExif.write((exif.length + 2) >> 8);
        withExif.write((exif.length + 2) & 0xFF);
        withExif.write(exif);
        withExif.write(jpeg, 2, jpeg.length - 2);

        StepVerifier.create(normalizer.normalize(withExif.toByteArray()))
                .assertNext(photo -> assertThat(new String(photo.content(), StandardCharsets.ISO_8859_1))
                        .doesNotContain("GPS-LATITUDE"))
                .verifyComplete();
    }

    private static String decodedSize(NormalizedPhoto photo) {
        try {
            BufferedImage decoded = ImageIO.read(new ByteArrayInputStream(photo.content()));
            return decoded.getWidth() + "x" + decoded.getHeight();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
