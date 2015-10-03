package co.cask.cdap.transforms.parser;

import co.cask.cdap.api.data.schema.Schema;

public final class TypeConvertors {
  
  public static Object get(String value, Schema.Type type) {
    Object object = null;
    
    switch(type) {
      case NULL:
        object = null;
        break;
      
      case STRING:
        object = value;
        break;
      
      case INT:
        object = getInt(value);
        break;
      
      case LONG:
        object = getLong(value);
        break;
      
      case DOUBLE:
        object = getDouble(value);
        break;
      
      case FLOAT:
        object = getFloat(value);
        break;
      
      case BOOLEAN:
        object = getBoolean(value);
        break;
      
      case BYTES:
        object = value.getBytes();
        break;
      
      case ENUM:
      case ARRAY:
      case MAP:
      case RECORD:
      case UNION:
        break;
        
    }
    return object;
    
  }
  
  private static int getInt(String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to convert '" + value + "' to INT");
    }
  }
  
  private static long getLong(String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to convert '" + value + "' to LONG");
    }
  }

  private static double getDouble(String value) {
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to convert '" + value + "' to DOUBLE");
    }
  }

  private static float getFloat(String value) {
    try {
      return Float.parseFloat(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to convert '" + value + "' to FLOAT");
    }
  }

  private static boolean getBoolean(String value) {
    try {
      return Boolean.parseBoolean(value);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to convert '" + value + "' to BOOLEAN");
    }
  }
  
}
