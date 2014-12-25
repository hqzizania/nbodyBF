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
package com.intel.nbody.Direct.Astro

import org.apache.spark.rdd.RDD
import com.intel.nbody.Direct._

/**
 *  Created by qhuang on 11/13/14.
 */



class DistNBodyDirectAstro (
//  sf: Softening Factor
//  dt: one time step
//  dm: damping
    private var nParticles: Int,
    private var slices: Int,
    private var cycles: Int,
    private var checkPointThreshold: Int,
    private var sf: Double,
    private var dt: Double,
    private var dm: Double) extends DistNBodyDirect {

  def this() = this(0, 0, 0, 0.01, 0.001, 0.995)

  def setSf(sf: Double): this.type = {
    if(sf <= 0) {
      throw new IllegalArgumentException("Softening Factor must be positive")
    }
    this.sf = sf
    this
  }
  def setDm(dt: Double): this.type = {
    if(dm <= 0 || dm >= 1) {
      throw new IllegalArgumentException("Damping must be defined on interval (0, 1)")
    }
    this.dm = dm
    this
  }
  def setDt(dt: Double): this.type = {
    if(dt <= 0) {
      throw new IllegalArgumentException("One time step must be positive")
    }
    this.dt = dt
    this
  }

  override private [nbody] def bodyBodyInteraction(a:Array[Double], b:Array[Double])={

    val rx = a(1) - b(1)
    val ry = a(2) - b(2)
    val rz = a(3) - b(3)
    var r = rx * rx + ry * ry + rz * rz
    r += sf
    // b(10) : b mass
    r = b(10) / math.sqrt(r * r * r) * b(0)

    // new accelerations
    a(4) += -rx * r
    a(5) += -ry * r
    a(6) += -rz * r

    a

  }

  override private [nbody] def newPosition(a:Array[Double])={

    // acceleration = force / mass;
    // new velocity = old velocity + acceleration * deltaTime
    a(7) += a(4) * dt
    a(8) += a(5) * dt
    a(9) += a(6) * dt

    a(7) *= dm
    a(8) *= dm
    a(9) *= dm

    // new position = old position + velocity * deltaTime
    a(1) += a(7) * dt
    a(2) += a(8) * dt
    a(3) += a(9) * dt

    // set all accelerations to zero
    a(4) = 0.0
    a(5) = 0.0
    a(6) = 0.0


    a

  }

}



object DistNBodyDirectAstro {

  def run(
           g: RDD[Array[Array[Double]]],
           nParticles: Int,
           slices: Int,
           cycles: Int,
           checkPointThreshold: Int,
           sf: Double,
           dt: Double,
           dm: Double) : RDD[Array[Array[Double]]] = {
    new DistNBodyDirectAstro()().setNParticles(nParticles)
      .setSlices(slices)
      .setCycles(cycles)
      .setSf(sf)
      .setDt(dt)
      .setDm(dm)
      .setCheckPointThreshold(checkPointThreshold)
      .simulation(g)
  }

  def run(
           g: RDD[Array[Array[Double]]],
           nParticles: Int,
           slices: Int,
           cycles: Int) : RDD[Array[Array[Double]]] = {
    new DistNBodyDirectAstro().setNParticles(nParticles)
      .setSlices(slices)
      .setCycles(cycles)
      .simulation(g)


  }

}