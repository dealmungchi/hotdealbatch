package kr.co.dealmungchi.hotdealbatch.listener;

import kr.co.dealmungchi.hotdealbatch.dto.HotDealDto;
import kr.co.dealmungchi.hotdealbatch.reactive.ReactiveHotDealProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedisSubscriberService implements MessageListener {

    private final ObjectMapper objectMapper;
    private final ReactiveHotDealProcessor reactiveProcessor;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            String base64 = new String(message.getBody(), StandardCharsets.UTF_8);
            String json = new String(Base64.getDecoder().decode(base64));
            HotDealDto[] dtosArray = objectMapper.readValue(json, HotDealDto[].class);

            Arrays.stream(dtosArray)
                .forEach(reactiveProcessor::push);
        } catch (Exception e) {
            log.error("Failed to parse message", e);
        }
    }

}
