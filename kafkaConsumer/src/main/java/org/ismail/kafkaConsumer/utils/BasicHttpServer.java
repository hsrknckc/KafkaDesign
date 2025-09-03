package org.ismail.kafkaConsumer.utils;

import fi.iki.elonen.NanoHTTPD;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class BasicHttpServer extends NanoHTTPD {
    private final File baseDir;

    public BasicHttpServer(int port, File baseDir) throws IOException {
        super(port);
        this.baseDir = baseDir;
        start(SOCKET_READ_TIMEOUT, false);
        System.err.println("Server started on port " + port);
    }

    @Override
    public Response serve(IHTTPSession session) {
        String uri = session.getUri();
        File file = new File(baseDir, uri);
        if (file.exists() &&  file.isFile()) {
            try{
                FileInputStream fis = new FileInputStream(file);

                Response response = newFixedLengthResponse(Response.Status.OK,"application/octet-stream",fis,file.length());
                response.addHeader("Content-Disposition","attachment; filename=\""+file.getName() + "\"");
                return response;
            } catch (Exception e) {
                return newFixedLengthResponse(Response.Status.INTERNAL_ERROR,"text/plain",e.getMessage());
            }
        }else {
            return newFixedLengthResponse(Response.Status.NOT_FOUND,"text/plain","Dosya bulunamadı");
        }
    }
}
