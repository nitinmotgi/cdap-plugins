package co.cask.cdap.transforms;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import com.google.common.collect.Lists;

import java.util.List;

public class MockEmitter<T> implements Emitter<T> {
  private final List<T> emitted = Lists.newArrayList();
  private final List<InvalidEntry<T>> errors = Lists.newArrayList();

  @Override
  public void emit(T value) {
    emitted.add(value);
  }

  @Override
  public void emitError(InvalidEntry<T> value) {
    errors.add(value);
  }

  public List<T> getEmitted() {
    return emitted;
  }
  public List<InvalidEntry<T>> getErrors() {
    return errors;
  }

  public void clear() {
    emitted.clear();
    errors.clear();
  }
}
