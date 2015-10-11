package co.cask.hydrator.transforms;

public enum EncodeDecodeType {
  STRING_BASE64("STRING_BASE64"),
  STRING_BASE32("STRING_BASE32"),
  BASE64("BASE64"),
  BASE32("BASE32"),
  HEX("HEX"),
  NONE("NONE");
  
  private String type;
  
  EncodeDecodeType(String type) {
    this.type = type;
  }
  
  String getType() {
    return type;
    
  }
}
