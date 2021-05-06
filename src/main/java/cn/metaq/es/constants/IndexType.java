package cn.metaq.es.constants;

public enum IndexType {

    DOC("_doc");

    private String type;

    IndexType(String type) {
        this.type = type;
    }

    public String getValue(){
        return type;
    }
}