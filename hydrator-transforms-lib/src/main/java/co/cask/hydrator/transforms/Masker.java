package co.cask.hydrator.transforms;


import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;

import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;

@Plugin(type = "transform")
@Name("Masker")
@Description("Masker masks string fields. Mask generated are of same length as the input field value.")
public class Masker extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  private String[] fieldsToMask;
  private long seed = System.currentTimeMillis();
  
  public Masker(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    if(config.seed != null && !config.seed.isEmpty()) {
      try {
        seed = Long.parseLong(config.seed);
      } catch (NumberFormatException e) {
        throw new RuntimeException("Seed specified '" + config.seed + ", is not a number.");
      }
    }
    fieldsToMask = config.fields.split(",");
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    
    try {
      if(config.seed != null && !config.seed.isEmpty()) {
        Long.parseLong(config.seed);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Randomizer seed specified '" + config.seed + "' is not a number");
    }
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(in.getSchema());
    
    List<Schema.Field> fields = in.getSchema().getFields();
    for(Schema.Field field : fields) {
      if (field.getSchema().getType() == Schema.Type.STRING && okToMask(field.getName())) {
        builder.set(field.getName(), mask((String)in.get(field.getName()), seed));  
      } else {
        builder.set(field.getName(), in.get(field.getName()));
      }
    }
    emitter.emit(builder.build());
  }

  private boolean okToMask(String name) {
    for(String field : fieldsToMask){
      if (name.equalsIgnoreCase(field)) {
        return true;
      }
    }
    return false;
  }
  
  private char randomChar (Random r, String cs, boolean uppercase) {
    char c = cs.charAt(r.nextInt(cs.length()));
    return uppercase ? Character.toUpperCase(c) : c;
  }

  private String mask (String str, long seed) {

    final String cons = "bcdfghjklmnpqrstvwxzBCDFGHJKLMNPQRSTVWXZ";
    final String vowel = "aeiouyAEIOUY";
    final String digit = "0123456789";

    Random r = new Random(seed);
    char data[] = str.toCharArray();

    for (int n = 0; n < data.length; ++ n) {
      char ln = Character.toLowerCase(data[n]);
      if (cons.indexOf(ln) >= 0)
        data[n] = randomChar(r, cons, ln != data[n]);
      else if (vowel.indexOf(ln) >= 0)
        data[n] = randomChar(r, vowel, ln != data[n]);
      else if (digit.indexOf(ln) >= 0)
        data[n] = randomChar(r, digit, ln != data[n]);
    }
    return new String(data);
  }


  public static class Config extends PluginConfig {
    @Name("seed")
    @Description("Specifies the seed to be used masking the fields.")
    @Nullable
    private final String seed;
    
    @Name("fields")
    @Description("List of fields to mask.")
    private final String fields;
    
    public Config(String seed, String fields) {
      this.seed = seed;
      this.fields = fields;
    }
    
  }
}
