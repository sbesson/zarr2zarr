/**
 * Copyright (c) 2024 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.zarr.test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.glencoesoftware.bioformats2raw.Converter;
import com.glencoesoftware.zarr.Convert;

import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConversionTest {

  private Path input;
  private Path output;
  private Converter converter;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  /**
   * Run the bioformats2raw main method and check for success or failure.
   *
   * @param additionalArgs CLI arguments as needed beyond "input output"
   */
  void assertBioFormats2Raw(String...additionalArgs) throws IOException {
    List<String> args = new ArrayList<String>();
    for (String arg : additionalArgs) {
      args.add(arg);
    }
    args.add(input.toString());
    output = tmp.newFolder().toPath().resolve("test");
    args.add(output.toString());
    try {
      converter = new Converter();
      CommandLine.call(converter, args.toArray(new String[]{}));
      Assert.assertTrue(Files.exists(output.resolve(".zattrs")));
      Assert.assertTrue(Files.exists(
        output.resolve("OME").resolve("METADATA.ome.xml")));
    }
    catch (RuntimeException rt) {
      throw rt;
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  static Path fake(String...args) {
    Assert.assertTrue(args.length %2 == 0);
    Map<String, String> options = new HashMap<String, String>();
    for (int i = 0; i < args.length; i += 2) {
      options.put(args[i], args[i+1]);
    }
    return fake(options);
  }

  static Path fake(Map<String, String> options) {
    return fake(options, null);
  }

  /**
   * Create a Bio-Formats fake INI file to use for testing.
   * @param options map of the options to assign as part of the fake filename
   * from the allowed keys
   * @param series map of the integer series index and options map (same format
   * as <code>options</code> to add to the fake INI content
   * @see https://docs.openmicroscopy.org/bio-formats/6.4.0/developers/
   * generating-test-images.html#key-value-pairs
   * @return path to the fake INI file that has been created
   */
  static Path fake(Map<String, String> options,
          Map<Integer, Map<String, String>> series)
  {
    return fake(options, series, null);
  }

  static Path fake(Map<String, String> options,
          Map<Integer, Map<String, String>> series,
          Map<String, String> originalMetadata)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("image");
    if (options != null) {
      for (Map.Entry<String, String> kv : options.entrySet()) {
        sb.append("&");
        sb.append(kv.getKey());
        sb.append("=");
        sb.append(kv.getValue());
      }
    }
    sb.append("&");
    try {
      List<String> lines = new ArrayList<String>();
      if (originalMetadata != null) {
        lines.add("[GlobalMetadata]");
        for (String key : originalMetadata.keySet()) {
          lines.add(String.format("%s=%s", key, originalMetadata.get(key)));
        }
      }
      if (series != null) {
        for (int s : series.keySet()) {
          Map<String, String> seriesOptions = series.get(s);
          lines.add(String.format("[series_%d]", s));
          for (String key : seriesOptions.keySet()) {
            lines.add(String.format("%s=%s", key, seriesOptions.get(key)));
          }
        }
      }
      Path ini = Files.createTempFile(sb.toString(), ".fake.ini");
      File iniAsFile = ini.toFile();
      String iniPath = iniAsFile.getAbsolutePath();
      String fakePath = iniPath.substring(0, iniPath.length() - 4);
      Path fake = Paths.get(fakePath);
      File fakeAsFile = fake.toFile();
      Files.write(fake, new byte[]{});
      Files.write(ini, lines);
      iniAsFile.deleteOnExit();
      fakeAsFile.deleteOnExit();
      return ini;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Test defaults.
   */
  @Test
  public void testDefaults() throws Exception {
    input = fake();
    assertBioFormats2Raw();

    // first convert v2 produced by bioformats2raw to v3
    Path v3Output = tmp.newFolder().toPath().resolve("v3-test");
    Convert v3Converter = new Convert();
    v3Converter.setInput(output.toString());
    v3Converter.setOutput(v3Output.toString());
    v3Converter.convertToV3();

    // TODO: test the v3 directly

    // now convert v3 back to v2
    Path roundtripOutput = tmp.newFolder().toPath().resolve("v2-roundtrip-test");
    Convert v2Converter = new Convert();
    v2Converter.setInput(v3Output.toString());
    v2Converter.setOutput(roundtripOutput.toString());
    v2Converter.setWriteV2(true);
    v2Converter.convertToV2();

    Path originalOMEXML = output.resolve("OME").resolve("METADATA.ome.xml");
    Path roundtripOMEXML = roundtripOutput.resolve("OME").resolve("METADATA.ome.xml");

    // make sure the OME-XML is present and not changed
    Assert.assertEquals(Files.readAllLines(originalOMEXML), Files.readAllLines(roundtripOMEXML));

    // since the image is small, make sure all pixels are identical in both resolutions
    ZarrArray originalFullResolution = ZarrGroup.open(output.resolve("0")).openArray("0");
    ZarrArray originalSubResolution = ZarrGroup.open(output.resolve("0")).openArray("1");

    ZarrArray roundtripFullResolution = ZarrGroup.open(roundtripOutput.resolve("0")).openArray("0");
    ZarrArray roundtripSubResolution = ZarrGroup.open(roundtripOutput.resolve("0")).openArray("1");

    compareZarrArrays(originalFullResolution, roundtripFullResolution);
    compareZarrArrays(originalSubResolution, roundtripSubResolution);
  }

  /**
   * Test simple plate.
   */
  @Test
  public void testPlate() throws Exception {
    input = fake("plateRows", "3", "plateCols", "4", "fields", "2");
    assertBioFormats2Raw();

    // first convert v2 produced by bioformats2raw to v3
    Path v3Output = tmp.newFolder().toPath().resolve("v3-plate-test");
    Convert v3Converter = new Convert();
    v3Converter.setInput(output.toString());
    v3Converter.setOutput(v3Output.toString());
    v3Converter.convertToV3();

    // TODO: test the v3 directly

    // now convert v3 back to v2
    Path roundtripOutput = tmp.newFolder().toPath().resolve("v2-plate-roundtrip-test");
    Convert v2Converter = new Convert();
    v2Converter.setInput(v3Output.toString());
    v2Converter.setOutput(roundtripOutput.toString());
    v2Converter.setWriteV2(true);
    v2Converter.convertToV2();

    Path originalOMEXML = output.resolve("OME").resolve("METADATA.ome.xml");
    Path roundtripOMEXML = roundtripOutput.resolve("OME").resolve("METADATA.ome.xml");

    // make sure the OME-XML is present and not changed
    Assert.assertEquals(Files.readAllLines(originalOMEXML), Files.readAllLines(roundtripOMEXML));

    // since the images are small, make sure all pixels are identical in both resolutions
    String[] groups = new String[] {
      "A/1/0", "A/1/1",
      "A/2/0", "A/2/1",
      "A/3/0", "A/3/1",
      "A/4/0", "A/4/1",
      "B/1/0", "B/1/1",
      "B/2/0", "B/2/1",
      "B/3/0", "B/3/1",
      "B/4/0", "B/4/1",
      "C/1/0", "C/1/1",
      "C/2/0", "C/2/1",
      "C/3/0", "C/3/1",
      "C/4/0", "C/4/1",
    };
    for (String group : groups) {
      for (int res=0; res<2; res++) {
        ZarrArray original = ZarrGroup.open(output.resolve(group)).openArray(String.valueOf(res));
        ZarrArray roundtrip = ZarrGroup.open(roundtripOutput.resolve(group)).openArray(String.valueOf(res));
        compareZarrArrays(original, roundtrip);
      }
    }
  }

  private void compareZarrArrays(ZarrArray original, ZarrArray roundtrip) throws Exception {
    Assert.assertArrayEquals(original.getShape(), roundtrip.getShape());

    int[] shape = original.getShape();
    byte[] originalImage = new byte[shape[3] * shape[4]];
    byte[] roundtripImage = new byte[shape[3] * shape[4]];
    original.read(originalImage, shape);
    roundtrip.read(roundtripImage, shape);

    Assert.assertArrayEquals(originalImage, roundtripImage);
  }

}
