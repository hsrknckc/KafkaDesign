package org.ismail.kafkaConsumer.utils;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.ismail.kafkaConsumer.HelloController;

public class LogCatcher extends AppenderBase<ILoggingEvent> {
    private final String keyword;

    public LogCatcher(String keyword) {
        this.keyword = keyword;
    }

    @Override
    protected void append(ILoggingEvent iLoggingEvent) {
        String msg = iLoggingEvent.getFormattedMessage();
        if(msg.contains(keyword)){
            HelloController.isListening.set(true);
        }
    }
}
