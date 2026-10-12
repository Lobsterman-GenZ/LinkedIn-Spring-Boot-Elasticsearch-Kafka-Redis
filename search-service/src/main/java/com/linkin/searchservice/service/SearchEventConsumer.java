package com.linkin.searchservice.service;

import com.linkin.searchservice.model.PostDocument;
import com.linkin.searchservice.model.UserDocument;
import com.linkin.searchservice.repository.PostSearchRepository;
import com.linkin.searchservice.repository.UserSearchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class SearchEventConsumer {
    private final UserSearchRepository userSearchRepository;
    private final PostSearchRepository postSearchRepository;

    @KafkaListener(topics = "user.created")
    public void consumeUserCreated(
            @Payload Map<String, Object> payload
            ){
        try {
            log.info("Indexing new user: {}", payload.get("userId"));
            UserDocument document = new UserDocument();
            document.setId((String) payload.get("userId"));
            document.setFirstName((String) payload.get("firstName"));
            document.setLastName((String) payload.get("lastName"));
            document.setEmail((String) payload.get("email"));
            document.setHeadLine((String) payload.get("headline"));
            document.setLocation((String) payload.get("location"));
            userSearchRepository.save(document);
            log.info("User indexed: {}", payload.get("userId"));
        } catch (Exception e) {
            log.error("Error indexing user: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void consumeUserUpdated(
            @Payload Map<String, Object> payload
    ){
        try {
            String userId = (String) payload.get("userId");
            log.info("Updating user index: {}", userId);
            userSearchRepository.findById(userId).ifPresentOrElse(userDocument -> {
                userDocument.setFirstName((String) payload.get("firstName"));
                userDocument.setLastName((String) payload.get("lastName"));
                userDocument.setHeadLine((String) payload.get("headline"));
                userDocument.setLocation((String) payload.get("location"));
                if (payload.get("skills") != null) {
                    userDocument.setSkills(asStringList(payload.get("skills")));
                }
                userSearchRepository.save(userDocument);
            },  () -> log.warn(
                    "User not found in Elasticsearch: {}",
                    userId
            ));

        } catch (Exception e) {
            log.error("Error updating user index: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }


    @KafkaListener(topics = "post.created")
    public void consumePostCreated(
            @Payload Map<String, Object> payload
    ){
        try {
            PostDocument document = new PostDocument();
            document.setId((String) payload.get("userId"));
            document.setAuthorId((String) payload.get("authorId"));
            document.setContent((String) payload.get("content"));
            document.setImageUrl((String) payload.get("imageUrl"));
            document.setCreatedAt((String) payload.get("createdAt"));
            postSearchRepository.save(document);
        } catch (Exception e) {
            log.error("Error indexing post: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static List<String> asStringList(Object value) {
        if (value instanceof List<?> list) {
            return list.stream().map(String::valueOf).toList();
        }
        return null;
    }
}
