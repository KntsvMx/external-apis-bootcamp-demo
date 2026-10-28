package com.accenture.externalapis.demo.client;

import com.accenture.externalapis.demo.config.ExternalServiceProperties;
import com.accenture.externalapis.demo.dto.BookApiResponse;
import com.accenture.externalapis.demo.dto.BookDto;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;


@Component
public class BookWebClientImpl implements BookWebClient {

    private WebClient webClient;

    public BookWebClientImpl(WebClient.Builder builder, ExternalServiceProperties properties) {
        this.webClient = builder
                .baseUrl(properties.baseUrl())
//                .defaultHeader("Authorization", "Bearer " + token)
                .build();
    }

    @Override
    public Mono<BookDto> getBookAsync(Long id) {
        Mono<BookApiResponse> bookApiResponseMono = webClient.get()
                .uri("/books/{id}", id)
                .retrieve()
                .onStatus(HttpStatusCode::isError, response ->
                        response.createException()
                                .map(originalException ->
                                        new ClientException("Error fetching book", originalException)))
                .bodyToMono(BookApiResponse.class)
                .onErrorMap(WebClientResponseException.class, originalException -> new ClientException("Error processing external API response", originalException))
                .onErrorMap(WebClientRequestException.class, originalException -> new ClientException("Error fetching book", originalException))
                .timeout(Duration.ofSeconds(5))
                .onErrorMap(TimeoutException.class, originalException ->
                        new ClientException("External API request timed out", originalException));

        return mapToBookDto(bookApiResponseMono);
    }


    @Override
    public Flux<BookDto> getAllBooksAsync() {
        Flux<BookApiResponse> bookApiResponseFlux = webClient.get()
                .uri("/books")
                .retrieve()
                .onStatus(HttpStatusCode::isError, response ->
                        response.createException()
                                .map(originalException -> new ClientException("Error Fetching book", originalException)))
                .bodyToFlux(BookApiResponse.class)
                .onErrorMap(WebClientResponseException.class, originalException -> new ClientException("Error processing external API response", originalException))
                .onErrorMap(WebClientRequestException.class, originalException -> new ClientException("Error fetching book", originalException))
                .timeout(Duration.ofSeconds(5))
                .onErrorMap(TimeoutException.class, originalException -> new ClientException("External API request timed out", originalException));

        return mapToBookDtos(bookApiResponseFlux);
    }

    @Override
    public Mono<List<BookDto>> getBooksInParallel(Long id1, Long id2) {
        Mono<BookDto> bookDtoMono1 = getBookAsync(id1);
        Mono<BookDto> bookDtoMono2 = getBookAsync(id2);

        return Mono.zip(bookDtoMono1, bookDtoMono2)
                .map(tuple -> List.of(tuple.getT1(), tuple.getT2()))
                .onErrorMap(error -> !(error instanceof ClientException),
                        error -> new ClientException("Error fetching books in parallel", error));
    }

    private Mono<BookDto> mapToBookDto(Mono<BookApiResponse> bookApiResponseMono) {
        return bookApiResponseMono.map(bookApiResponse -> new BookDto(bookApiResponse.title(), bookApiResponse.author(), bookApiResponse.genre(), bookApiResponse.price()));
    }

    private Flux<BookDto> mapToBookDtos(Flux<BookApiResponse> bookApiResponseFlux) {
        return bookApiResponseFlux.map(bookApiResponse -> new BookDto(bookApiResponse.title(), bookApiResponse.author(), bookApiResponse.genre(), bookApiResponse.price()));
    }

}
