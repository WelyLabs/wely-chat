package com.calendar.chat.configuration;

import com.calendar.chat.exception.RequestIds;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.metadata.CompositeMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.util.DefaultPayload;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.util.MimeTypeUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RSocketMetadataConfigTest {

    @Test
    void requestIdMetadata_shouldExtractTheClientsRequestId_fromCompositeMetadata() {
        RSocketStrategies.Builder builder = RSocketStrategies.builder();
        new RSocketMetadataConfig().requestIdMetadata().customize(builder);
        RSocketStrategies strategies = builder.build();

        CompositeByteBuf metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        CompositeMetadataCodec.encodeAndAddMetadata(metadata, ByteBufAllocator.DEFAULT,
                RequestIds.REQUEST_ID_MIME_TYPE.toString(),
                Unpooled.copiedBuffer("abcd1234-ef56", StandardCharsets.UTF_8));

        Map<String, Object> extracted = strategies.metadataExtractor().extract(
                DefaultPayload.create(Unpooled.EMPTY_BUFFER, metadata),
                MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getString()));

        assertThat(extracted).containsEntry(RequestIds.REQUEST_ID_ATTRIBUTE, "abcd1234-ef56");
    }
}
