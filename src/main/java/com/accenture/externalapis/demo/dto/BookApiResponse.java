package com.accenture.externalapis.demo.dto;

public record BookApiResponse(long id,
                              String title,
                              String author,
                              String genre,
                              double price,
                              String isbn,
                              int publishedYear) {
}
