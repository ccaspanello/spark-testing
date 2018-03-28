package com.github.ccaspanello.spark.etl;

import com.github.ccaspanello.spark.etl.api.Step;
import com.github.ccaspanello.spark.etl.api.StepMeta;
import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Step Registry
 *
 * Scans the classpath for all classes that extend the BaseStep implementation.  Using reflection we create a map to
 * lookup the logic class based on the meta class.
 *
 * TODO Revisit this; is there a cleaner way that uses annotations.
 *
 * Created by ccaspanello on 1/29/18.
 */
public class StepRegistry implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger( StepRegistry.class );
  private Map<Class<? extends StepMeta>, Class<? extends Step>> registry;

  public StepRegistry() {
    registry = new HashMap<>();
  }

  public void init() {
    try {
      Reflections reflections = new Reflections( Main.class.getPackage().getName() );
      Set<Class<? extends BaseStep>> clazzes = reflections.getSubTypesOf( BaseStep.class );
      for ( Class<? extends BaseStep> stepClazz : clazzes ) {
        ParameterizedType clazzType = (ParameterizedType) stepClazz.getGenericSuperclass();
        Class metaClazz = Class.forName( clazzType.getActualTypeArguments()[ 0 ].getTypeName() );
        registry.put( metaClazz, stepClazz );
      }
    } catch ( ClassNotFoundException e ) {
      throw new RuntimeException( "Unexpected error trying to initialize step registry.", e );
    }
  }

  public <T extends Step> T createStep( StepMeta stepMeta ) {
    Class<? extends Step> clazz = registry.get( stepMeta.getClass() );
    try {
      Constructor constructor = clazz.getConstructor( stepMeta.getClass() );
      return (T) constructor.newInstance( stepMeta );
    } catch ( NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException e ) {
      throw new RuntimeException( "Unexpected error trying to instantiate Step: ", e );
    }
  }

  Map<Class<? extends StepMeta>, Class<? extends Step>> getRegistry() {
    return registry;
  }

}
