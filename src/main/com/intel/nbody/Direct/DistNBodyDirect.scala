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
package com.intel.nbody.Direct

import org.apache.spark.{Logging => Logg}
import scala.math._
import org.apache.spark.rdd.RDD

/**
 *  Created by qhuang on 11/13/14.
 */


class DistNBodyDirect (
    private var nParticles: Int,
    private var slices: Int,
    private var cycles: Int,
    private var checkPointThreshold: Int) extends Serializable with Logg{

  def this() = this(0, 0, 0, 0.01)

  /** Set the support threshold. Support threshold must be defined on interval [0, 1]. Default: 0. */
  def setNParticles(nParticles: Int): this.type = {
    if(nParticles < 1) {
      throw new IllegalArgumentException("number of particles must be larger than 0")
    }
    this.nParticles = nParticles
    this
  }

  def setSlices(slices: Int): this.type = {
    if(slices < 1) {
      throw new IllegalArgumentException("number of slices must be larger than 0")
    }
    this.slices = slices
    this
  }

  def setCycles(cycles: Int): this.type = {
    if(cycles < 1) {
      throw new IllegalArgumentException("number of cycles must be larger than 0")
    }
    this.cycles = cycles
    this
  }


  def setCheckPointThreshold(checkPointThreshold: Int): this.type = {
    if(checkPointThreshold < 1) {
      throw new IllegalArgumentException("number of CheckPoint Threshold must be larger than 0")
    }
    this.checkPointThreshold = checkPointThreshold
    this
  }

  def simulation(parInfoI:RDD[Array[Array[Double]]]): RDD[Array[Array[Double]]] = {
    var parInfo = parInfoI
    val sc = parInfo.sparkContext
    var bcParInfo = sc.broadcast(parInfo.collect())
    val pathRDD = "nParticles_location.RDD.txt"

    checkWrite(pathRDD, bcParInfo.value)

    for (k <- 0 until cycles) {
      parInfo = parInfoMap(parInfo, bcParInfo.value)

      if (k % checkPointThreshold == (checkPointThreshold - 1) || k == cycles - 1) {
        // Cut the lineage ! To prevent from StackOverFlowError
        parInfo.checkpoint()
        parInfo.count()

        parInfo.saveAsTextFile(pathRDD)
        if(checkWrite(pathRDD, parInfo.collect())){
          throw new InterruptedException("This N-body simulation is interrupted at iteration " + k + " !")
        }
      }
      bcParInfo = sc.broadcast(parInfo.collect())
    }

    logInfo("iteration number: " + cycles)
    logInfo("This N-body simulation is completed!")
    logInfo("*****************************************")

    parInfo

  }

  private [nbody] def parInfoMap(parInfoI:RDD[Array[Array[Double]]], bcParInfoI:Array[Array[Array[Double]]])
    : RDD[Array[Array[Double]]] = {
    parInfoI.map(p => update(p, bcParInfoI)).cache()
  }

  private [nbody] def update(a: Array[Array[Double]], b: Array[Array[Array[Double]]]) = {
    newPositionMatrix(
      nBodyInteraction(a,  b))
  }

  private [nbody] def checkWrite(pathRDD: String, a: Array[Array[Array[Double]]]): Boolean = { false }

  private [nbody] def nBodyInteraction(a:Array[Array[Double]], b:Array[Array[Array[Double]]])={
    // a: local particles, b: global particles
    // Compute forces between a and b
    for (i <- 0 until a.size){
      for (j <- 0 until b(0).size * b.size){
        val bi = j / b(0).size
        val bj = j % b(0).size
        if(abs(a(i)(0) - b(bi)(bj)(0)) > 1e-6){
          a(i) = bodyBodyInteraction(a(i), b(bi)(bj))
        }
      }
    }
    a
  }

  private [nbody] def bodyBodyInteraction(a:Array[Double], b:Array[Double]): Array[Double] = { a }

  private [nbody] def newPositionMatrix(a:Array[Array[Double]])={
    for( i <- 0 until a.size ){
      a(i) = newPosition(a(i))
    }
    a

  }

  private [nbody] def newPosition(a:Array[Double]): Array[Double] = { a }

}




