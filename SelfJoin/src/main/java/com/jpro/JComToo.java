package com.jpro;

import lombok.extern.log4j.Log4j2;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

@Log4j2
class JComToo {
    static List<String> parseCsv(String seq) {
        log.info("seq: " + seq);
        List<String> _r = new LinkedList<>();
        StringBuilder term = new StringBuilder();
        for (int i = 0; i < seq.length(); i++) {
            if (seq.charAt(i) == ' ') {
                if (!term.toString().isEmpty()) {
                    _r.add(term.toString());
                    term.delete(0, term.length());
                }
                continue;
            } else if (seq.charAt(i) == ',') {
                if (!term.toString().isEmpty()) {
                    _r.add(term.toString());
                    term.delete(0, term.length());
                }
                continue;
            }
            term.append(seq.charAt(i));
            if (i == seq.length() - 1 && !term.toString().isEmpty()) {
                _r.add(term.toString());
            }
        }
        return _r;
    }

    static void checkProperty(Properties p, String key) {
        if (!p.containsKey(key)) {
            log.error("No such key [ kafka.ip ] in properties, exiting.");
            System.exit(-1);
        }
    }
}
