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

package io.github.zhztheplayer.velox4j.integration;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.Assignment;
import io.github.zhztheplayer.velox4j.connector.ColumnType;
import io.github.zhztheplayer.velox4j.connector.FileFormat;
import io.github.zhztheplayer.velox4j.connector.HiveColumnHandle;
import io.github.zhztheplayer.velox4j.connector.HiveConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.HiveTableHandle;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.jni.Session;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.tests.ResourceTests;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

public class QueryTest {

  @Test
  public void testScan() {
    // 1. Initialize Velox4j.
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
            false,
            List.of(),
            null,
            outputType,
            Map.of()
        ),
        toAssignments(outputType)
    );

    // 4. Create a split associating with the table scan node, this makes
    // the scan read a local file "/tmp/nation.parquet".
    final File file = ResourceTests.copyResourceToTmp("data/nation.parquet");
    final BoundSplit split = new BoundSplit(
        scanNode.getId(),
        -1,
        new HiveConnectorSplit(
            "connector-hive",
            0,
            false,
            file.getAbsolutePath(),
            FileFormat.PARQUET,
            0,
            file.length(),
            Map.of(),
            OptionalInt.empty(),
            Optional.empty(),
            Map.of(),
            Optional.empty(),
            Map.of(),
            Map.of(),
            Optional.empty(),
            Optional.empty()
        )
    );

    // 5. Build the query.
    final Query query = new Query(scanNode, List.of(split), Config.empty(), ConnectorConfig.empty());

    // 6. Create a Velox4j session.
    final MemoryManager memoryManager = MemoryManager.create(AllocationListener.NOOP);
    final Session session = Session.create(memoryManager);

    // 7. Execute the query.
    final UpIterator itr = session.executeQuery(query);

    // 8. Collect and print results.
    int i = 0;
    while (itr.hasNext()) {
      final RowVector rowVector = itr.next(); // 8.1. Get next RowVector returned by Velox.
      final VectorSchemaRoot vsr = session.arrowOps().toArrowTable(new RootAllocator(), rowVector).toVectorSchemaRoot(); // 8.2. Convert the RowVector into Arrow format (an Arrow VectorSchemaRoot in this case).
      final String expectedOutput = ResourceTests.readResourceAsString(String.format("output/nation-%d.tsv", i++));
      Assert.assertEquals(expectedOutput, vsr.contentToTSVString()); // 8.3. Verify the result.
      vsr.close(); // 8.4. Release the Arrow VectorSchemaRoot.
    }

    // 9. Close the Velox4j session.
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
