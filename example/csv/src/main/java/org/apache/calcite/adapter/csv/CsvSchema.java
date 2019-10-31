/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.csv;

import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharSink;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class CsvSchema extends AbstractSchema {
  private final File directoryFile;
  private final CsvTable.Flavor flavor;
  private Map<String, Table> tableMap;

  /**
   * Creates a CSV schema.
   *
   * @param directoryFile Directory that holds {@code .csv} files
   * @param flavor     Whether to instantiate flavor tables that undergo
   *                   query optimization
   */
  public CsvSchema(File directoryFile, CsvTable.Flavor flavor) {
    super();
    this.directoryFile = directoryFile;
    this.flavor = flavor;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or the original string. */
  private static String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    return trimmed != null ? trimmed : s;
  }

  /** Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or null. */
  private static String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  @Override protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = createTableMap();
    }
    return tableMap;
  }

  private Map<String, Table> createTableMap() {
    // Look for files in the directory ending in ".csv", ".csv.gz", ".json",
    // ".json.gz".
    final Source baseSource = Sources.of(directoryFile);
    File[] files = directoryFile.listFiles((dir, name) -> {
      final String nameSansGz = trim(name, ".gz");
      return nameSansGz.endsWith(".csv")
          || nameSansGz.endsWith(".json");
    });
    if (files == null) {
      System.out.println("directory " + directoryFile + " not found");
      files = new File[0];
    }
    // Build a map from table name to table; each file becomes a table.
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (File file : files) {
      Source source = Sources.of(file);
      Source sourceSansGz = source.trim(".gz");
      final Source sourceSansJson = sourceSansGz.trimOrNull(".json");
      if (sourceSansJson != null) {
        final Table table = new JsonScannableTable(source);
        builder.put(sourceSansJson.relative(baseSource).path(), table);
      }
      final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
      if (sourceSansCsv != null) {
        final Table table = createTable(source);
        builder.put(sourceSansCsv.relative(baseSource).path(), table);
      }
    }
    return builder.build();
  }

  /** Creates different sub-type of table based on the "flavor" attribute. */
  private Table createTable(Source source) {
    switch (flavor) {
    case TRANSLATABLE:
      return new CsvTranslatableTable(source, null, getStatistic(source));
    case SCANNABLE:
      return new CsvScannableTable(source, null, getStatistic(source));
    case FILTERABLE:
      return new CsvFilterableTable(source, null, getStatistic(source));
    default:
      throw new AssertionError("Unknown flavor " + this.flavor);
    }
  }

  private Statistic getStatistic(Source source) {
    Statistic statistic = null;
    try {
      final String statfilename = source.file().getAbsolutePath().concat(".stat");
      statistic = readStatisticFromFile(statfilename);
      if (statistic != null) {
        final Stream<String> lines = Files.lines(Paths.get(source.file().toURI()));
        statistic = Statistics.of(lines.count(), ImmutableList.of());
        writeStatisticFile(statistic,
                Paths.get(source.file().getAbsolutePath().concat(".stat")));
      }
    } catch (IOException ignored) {
    }
    return statistic;
  }

  private Statistic readStatisticFromFile(String filename) throws IOException {
    Path path = Paths.get(filename);
    try {
      if (Files.exists(Paths.get(filename))) {
        final String firstLine = com.google.common.io.Files.
                readFirstLine(path.toFile(), Charset.defaultCharset());
        if (firstLine != null) {
          final String[] split = firstLine.trim().split(":");
          if (split.length == 2) {
            double rows = Double.parseDouble(split[1]);
            return Statistics.of(rows, ImmutableList.of());
          }
        }
      }
    } catch (Exception ignored) {
    }
    return null;
  }

  private void writeStatisticFile(Statistic statistic, Path path) throws IOException {
    if (!Files.exists(path)) {
      final CharSink charSink = com.google.common.io.Files
              .asCharSink(path.toFile(), Charset.defaultCharset());
      charSink.write("rows:" + statistic.getRowCount().toString());
    }
  }

}

// End CsvSchema.java
