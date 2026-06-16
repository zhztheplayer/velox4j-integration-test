/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.boostscale.velox4j.integration;

import org.boostscale.velox4j.Velox4j;
import org.boostscale.velox4j.arrow.Arrow;
import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.config.ConnectorConfig;
import org.boostscale.velox4j.connector.*;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.iterator.ExportIterators;
import org.boostscale.velox4j.memory.AllocationListener;
import org.boostscale.velox4j.memory.MemoryManager;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.query.SerialTask;
import org.boostscale.velox4j.session.Session;
import org.boostscale.velox4j.test.ResourceTests;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.Type;
import org.boostscale.velox4j.type.VarCharType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;

import java.io.File;
import java.util.*;

public class QueryTest {
  @Test
  public void testScan() {
    // 1. Initialize Velox4J.
    Velox4j.initialize();

    // 2. Define the plan output schema.
    final RowType outputType = new RowType(List.of(
        "n_nationkey",
        "n_name",
        "n_regionkey",
        "n_comment"
    ), List.of(
        new BigIntType(),
        new VarCharType(),
        new BigIntType(),
        new VarCharType()
    ));

    // 3. Create a table scan node.
    final TableScanNode scanNode = new TableScanNode(
        "plan-id-1",
        outputType,
        new HiveTableHandle(
            "connector-hive",
            "table-1",
            List.of(),
            null,
            outputType,
            Map.of()
        ),
        toAssignments(outputType)
    );

    // 4. Build the query.
    final Query query = new Query(scanNode, Config.empty(), ConnectorConfig.empty());

    // 5. Create a Velox4J session.
    final MemoryManager memoryManager = Velox4j.newMemoryManager(AllocationListener.NOOP);
    final Session session = Velox4j.newSession(memoryManager);

    // 6. Execute the query. A Velox serial task will be returned.
    final SerialTask task = session.queryOps().execute(query);

    // 7. Add a split associating with the table scan node to the task, this makes
    // the scan read a local file "/tmp/nation.parquet".
    final File file = ResourceTests.copyResourceToTmp("data/nation.parquet");
    final ConnectorSplit split = new HiveConnectorSplit(
        "connector-hive",
        0,
        false,
        file.getAbsolutePath(),
        FileFormat.PARQUET,
        0,
        file.length(),
        Map.of(),
        null,
        null,
        Map.of(),
        null,
        Map.of(),
        Map.of(),
        null,
        null
    );
    task.addSplit(scanNode.getId(), split);
    task.noMoreSplits(scanNode.getId());

    // 8. Create a Java iterator from the Velox task.
    final Iterator<RowVector> itr = ExportIterators.asJavaIterator(task);

    // 9. Collect and print results.
    while (itr.hasNext()) {
      final RowVector rowVector = itr.next(); // 9.1. Get next RowVector returned by Velox.
      final VectorSchemaRoot vsr = Arrow.toArrowVectorSchemaRoot(new RootAllocator(), rowVector); // 9.2. Convert the RowVector into Arrow format (an Arrow VectorSchemaRoot in this case).
      System.out.println(vsr.contentToTSVString()); // 9.3. Print the arrow table to stdout.
      vsr.close(); // 9.4. Release the Arrow VectorSchemaRoot.
    }

    // 10. Close the Velox4J session.
    session.close();
    memoryManager.close();
  }

  private static List<Assignment> toAssignments(RowType rowType) {
    final List<Assignment> list = new ArrayList<>();
    for (int i = 0; i < rowType.size(); i++) {
      final String name = rowType.getNames().get(i);
      final Type type = rowType.getChildren().get(i);
      list.add(new Assignment(name,
          new HiveColumnHandle(name, ColumnType.REGULAR, type, type, List.of())));
    }
    return list;
  }
}
