package org.ismail.kafkaProducer.utils;

import org.jetbrains.annotations.NotNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Utilities {
    @NotNull
    public static String getFileType(String filePath) throws IOException {
        Path path= Paths.get(filePath);
        String mimeType = Files.probeContentType(path);
        if(mimeType==null) return "unknown";

        if(mimeType.startsWith("text") ) return "text";
        else if(mimeType.startsWith("image") ) return "image";
        else if(mimeType.startsWith("video") ) return "video";
        else if(mimeType.startsWith("audio") ) return "audio";
        else return mimeType;
    }
}
