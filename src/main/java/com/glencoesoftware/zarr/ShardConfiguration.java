/**
 * Copyright (c) 2024 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.zarr;

public enum ShardConfiguration {
  SINGLE,
  CHUNK,
  SUPERCHUNK,
  CUSTOM;
}
