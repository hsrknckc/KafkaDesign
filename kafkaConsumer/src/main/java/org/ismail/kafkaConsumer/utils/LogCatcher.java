package org.ismail.kafkaConsumer.utils;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import javafx.application.Platform;

public class LogCatcher extends AppenderBase<ILoggingEvent> {
    private final String keyword;
    private final Runnable onCatch;

    public LogCatcher(String keyword,Runnable onCatch) {
        this.keyword = keyword;
        this.onCatch = onCatch;
    }

    @Override
    protected void append(ILoggingEvent iLoggingEvent) {
        String msg = iLoggingEvent.getFormattedMessage();
        if(msg.contains(keyword)){
            System.out.println("YAKALANDIN");
            if(onCatch!=null){
                Platform.runLater(onCatch);
            }
        }
    }
}
