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

        if(mimeType.startsWith("text") ) {System.out.println(mimeType); return "text";}
        else if(mimeType.startsWith("image") ) {System.out.println(mimeType); return "image";}
        else if(mimeType.startsWith("video") ) {System.out.println(mimeType); return "video";}
        else if(mimeType.startsWith("audio") ) {System.out.println(mimeType); return "audio";}
        else {System.out.println(mimeType); return "desteklenmeyen dosya tipi";}
    }

}
