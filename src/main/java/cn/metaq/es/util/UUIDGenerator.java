package cn.metaq.es.util;

import java.util.UUID;

public class UUIDGenerator {

    public static String generate(){
        String uuid=UUID.randomUUID().toString();
        return uuid.replace("-","");
    }

}