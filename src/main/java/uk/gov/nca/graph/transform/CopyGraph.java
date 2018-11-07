/*
National Crime Agency (c) Crown Copyright 2018

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package uk.gov.nca.graph.transform;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.nca.graph.utils.GraphUtils;

/**
 * Utility to help copy one graph into another.
 *
 * This is done by temporarily writing the source graph into a Gryo file, and then
 * reading this back into the target graph.
 */
public class CopyGraph {
  private static final Logger LOGGER = LoggerFactory.getLogger(CopyGraph.class);

  private CopyGraph(){
    //Do nothing
  }

  /**
   * Copies the source graph into the target graph. If dropExisting is set to true,
   * then any existing data in the target graph is first removed.
   */
  public static boolean copyGraph(Graph source, Graph target, boolean dropExisting) {
    if(dropExisting){
      LOGGER.info("Clearing existing target graph");
      GraphUtils.clearGraph(target);
    }else{
      if(target.vertices().hasNext() || target.edges().hasNext()){
        LOGGER.error("Target graph must be empty, or dropExisting set to true");
        return false;
      }
    }

    LOGGER.info("Creating temporary file");

    File tempFile;
    try {
      tempFile = Files.createTempFile("copygraph", ".gryo").toFile();
    }catch (IOException ioe){
      LOGGER.error("Unable to create temporary file", ioe);
      return false;
    }
    tempFile.deleteOnExit();
    LOGGER.debug("Temporary file {} created", tempFile.getAbsolutePath());

    LOGGER.info("Reading into Gryo file from source graph");
    boolean readResult = GraphUtils.writeGraphFile(tempFile, "gryo", source);
    if(!readResult){
      LOGGER.error("Unable to write to Gryo file");
      return false;
    }

    LOGGER.info("Writing to target graph from Gryo file");
    boolean writeResult = GraphUtils.readGraphFile(tempFile, "gryo", target);

    if(!writeResult)
      LOGGER.error("Unable to read from Gryo file");

    return writeResult;
  }
}
