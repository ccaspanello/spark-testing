package com.github.mini.cluster.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class FileOperation {
  // Logger
  private static final Logger LOG = LoggerFactory.getLogger( FileOperation.class );

  //messages
  private static final String DELETING_CONTENTS = "Deleting contents of directory: {}";
  private static final String REMOVING_FILE = "Removing file: {}";
  private static final String REMOVING_DIRECTORY = "Removing directory: {}";
  private static final String UNABLE_REMOVE = "Unable to remove {}";

  public static void deleteFolder( String directory, boolean quietly ) {
    try {
      Path directoryPath = Paths.get( directory ).toAbsolutePath();
      if ( !quietly ) {
        LOG.info(  DELETING_CONTENTS,
          directoryPath.toAbsolutePath().toString() );
      }
      Files.walkFileTree( directoryPath, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile( Path file, BasicFileAttributes attrs )
          throws IOException {
          Files.delete( file );
          if ( !quietly ) {
            LOG.info( REMOVING_FILE, file.toAbsolutePath().toString() );
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory( Path dir, IOException exc )
          throws IOException {
          Files.delete( dir );
          if ( !quietly ) {
            LOG.info( REMOVING_DIRECTORY , dir.toAbsolutePath().toString() );
          }
          return FileVisitResult.CONTINUE;
        }
      } );
    } catch ( IOException e ) {
      LOG.error( UNABLE_REMOVE , directory );
    }
  }

  public static void deleteFolder( String directory ) {
    deleteFolder( directory, false );
  }

  public static String createTemporaryDirectory( String dirPrefix ) throws IOException {
    File currentDir = new File(System.getProperty( "user.dir" ));
    File file = new File(currentDir.getAbsolutePath(), dirPrefix);
    file.mkdir();
    return file.getAbsolutePath();
  }
}
