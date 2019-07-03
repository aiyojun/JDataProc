package com.jpro;

import org.bson.Document;

public interface Filter {
    Document doFilter(Document orig);
}
