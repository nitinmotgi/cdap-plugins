package co.cask.hydrator.transforms;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;

@Plugin(type = "transform")
@Name("Tokenizer")
@Description("Tokenizes the string.")
public final class Tokenizer extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  
  public Tokenizer(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
  }
  
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    
  }


  
  public static class Config extends PluginConfig {
    @Name("field")
    @Description("Field to be used for tokenizing")
    private final String field;
    
    @Name("stopword")
    @Description("List of stop words")
    
    public Config(String field) {
      this.field = field;
    }
    
  }
}
