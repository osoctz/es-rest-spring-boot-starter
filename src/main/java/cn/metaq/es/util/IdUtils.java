package cn.metaq.es.util;

public abstract class IdUtils {

    public static String uuid() {
        return UUIDGenerator.generate();
    }

    public static String objectId() {
        //mongo的 ObjectId.get().toHexString()
        return UUIDGenerator.generate();
    }
}
