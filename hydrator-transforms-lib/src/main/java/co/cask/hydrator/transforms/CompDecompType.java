package co.cask.hydrator.transforms;

public enum CompDecompType {
  SNAPPY("STRING_BASE64"),
  ZIP("STRING_BASE32"),
  GZIP("BASE64"),
  NONE("NONE");

  private String type;

  CompDecompType(String type) {
    this.type = type;
  }
  
  String getType() {
    return type;
    
  }
}
