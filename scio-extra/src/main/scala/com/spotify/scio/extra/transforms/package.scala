/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.extra

import java.io.File
import java.net.URI
import java.nio.file.Path

import com.spotify.scio.util.Functions
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.ParDo

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Main package for transforms APIs. Import all.
 */
package object transforms {

  /**
   * Enhanced version of [[SCollection]] with [[URI]] methods.
   */
  implicit class URISCollection(val self: SCollection[URI]) extends AnyVal {

    /**
     * Download [[URI]] elements and process as local [[Path]]s.
     */
    def mapFile[T: ClassTag](f: Path => T): SCollection[T] =
      self.applyTransform(ParDo.of(new FileDoFn[T](Functions.serializableFn(f))))

    /**
     * Download [[URI]] elements and process as local [[Path]]s.
     */
    def flatMapFile[T: ClassTag](f: Path => TraversableOnce[T]): SCollection[T] =
      self
        .applyTransform(ParDo.of(new FileDoFn[TraversableOnce[T]](Functions.serializableFn(f))))
        .flatMap(identity)

  }

  /**
   * Enhanced version of [[SCollection]] with pipe methods.
   */
  implicit class PipeSCollection(val self: SCollection[String]) extends AnyVal {

    /**
     * Pipe elements through an external command via StdIn & StdOut.
     * @param command the command to call
     * @param environment environment variables
     * @param dir the working directory of the sub-process
     */
    def pipe(command: String,
             environment: Map[String, String] = Map.empty,
             dir: File = null): SCollection[String] =
      self.applyTransform(ParDo.of(new PipeDoFn(command, environment.asJava, dir)))

    /**
     * Pipe elements through an external command via StdIn & StdOut.
     * @param cmdArray array containing the command to call and its arguments
     * @param environment environment variables
     * @param dir the working directory of the sub-process
     */
    def pipe(cmdArray: Array[String],
             environment: Map[String, String],
             dir: File): SCollection[String] =
      self.applyTransform(ParDo.of(new PipeDoFn(cmdArray, environment.asJava, dir)))

  }

}
