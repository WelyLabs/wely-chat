package com.calendar.chat.infrastructure.media.adapters;

import com.calendar.chat.domain.models.NormalizedPhoto;
import com.calendar.chat.domain.ports.PhotoNormalizer;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * Normalizes conversation photos with the JDK's ImageIO: JPEG, PNG, GIF and BMP in, a JPEG out,
 * its proportions kept and its longest side at most {@link #MAX_SIDE}.
 *
 * <p>Decoding is the type check, and the size is read from the header before the pixels, so a
 * small file claiming a huge image is refused without being decoded. The photo is drawn anew and
 * encoded as JPEG, which carries none of the original's metadata — the GPS position included.
 * ImageIO does not apply the EXIF orientation: the web app draws the photo on a canvas before
 * sending, which does.
 *
 * <p>ImageIO blocks: the work runs on boundedElastic, off the event loop.
 */
@Component
public class ImageIoPhotoNormalizerAdapter implements PhotoNormalizer {

    static final int MAX_SIDE = 1600;
    static final int MAX_SOURCE_SIDE = 8000;
    /** Decoded, a photo takes 4 bytes per pixel: 40 megapixels is 160 MB, the most worth holding. */
    static final long MAX_SOURCE_PIXELS = 40_000_000L;
    static final float QUALITY = 0.85f;
    static final String CONTENT_TYPE = "image/jpeg";

    @Override
    public Mono<NormalizedPhoto> normalize(byte[] upload) {
        return Mono.fromCallable(() -> {
                    BufferedImage photo = bounded(read(upload));
                    return new NormalizedPhoto(encode(photo), CONTENT_TYPE, photo.getWidth(), photo.getHeight());
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private static BufferedImage read(byte[] upload) throws IOException {
        try (ImageInputStream input = ImageIO.createImageInputStream(new ByteArrayInputStream(upload))) {
            Iterator<ImageReader> readers = input == null ? null : ImageIO.getImageReaders(input);
            if (readers == null || !readers.hasNext()) {
                throw invalid();
            }
            ImageReader reader = readers.next();
            try {
                reader.setInput(input, true, true);
                int width = reader.getWidth(0);
                int height = reader.getHeight(0);
                if (width > MAX_SOURCE_SIDE || height > MAX_SOURCE_SIDE || (long) width * height > MAX_SOURCE_PIXELS) {
                    throw invalid();
                }
                return reader.read(0);
            } catch (IOException | RuntimeException unreadable) {
                if (unreadable instanceof ChatException chat) {
                    throw chat;
                }
                throw invalid();
            } finally {
                reader.dispose();
            }
        }
    }

    /** Scaled down so the longest side fits, never up; on an opaque canvas, since JPEG has no transparency. */
    private static BufferedImage bounded(BufferedImage source) {
        double scale = Math.min(1.0, (double) MAX_SIDE / Math.max(source.getWidth(), source.getHeight()));
        int width = Math.max(1, (int) Math.round(source.getWidth() * scale));
        int height = Math.max(1, (int) Math.round(source.getHeight() * scale));

        BufferedImage target = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D graphics = target.createGraphics();
        try {
            graphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
            graphics.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
            graphics.setColor(Color.WHITE);
            graphics.fillRect(0, 0, width, height);
            graphics.drawImage(source, 0, 0, width, height, null);
        } finally {
            graphics.dispose();
        }
        return target;
    }

    private static byte[] encode(BufferedImage image) throws IOException {
        ImageWriter writer = ImageIO.getImageWritersByFormatName("jpeg").next();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ImageOutputStream output = ImageIO.createImageOutputStream(bytes)) {
            writer.setOutput(output);
            ImageWriteParam params = writer.getDefaultWriteParam();
            params.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            params.setCompressionQuality(QUALITY);
            writer.write(null, new IIOImage(image, null, null), params);
        } finally {
            writer.dispose();
        }
        return bytes.toByteArray();
    }

    private static ChatException invalid() {
        return new ChatException(ChatErrorCode.PHOTO_INVALID);
    }
}
