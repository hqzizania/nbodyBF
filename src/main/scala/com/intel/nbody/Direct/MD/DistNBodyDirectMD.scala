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
package com.intel.nbody.Direct.MD

import scala.math._
import org.apache.spark.rdd.RDD
import com.intel.nbody.Direct._

/**
 *  Created by qhuang on 11/13/14.
 */




class DistNBodyDirectMD (
//  L: linear size of cubical volume
//  dt: one time step
//  Verlet cutoff on the potential and neighbor list approximation
//  rCutoff = Cutoff * Cutoff
    private var nParticles: Int,
    private var slices: Int,
    private var cycles: Int,
    private var L: Double,
    private var checkPointThreshold: Int,
    private var dt: Double,
    private var rCutoff: Double) extends DistNBodyDirect {

  def this() = this(0, 0, 0, 150, 0, 0.01, 2.5*2.5)

  def setL(L: Double): this.type = {
    if(L <= 0) {
      throw new IllegalArgumentException("Linear size of cubical volume must be positive")
    }
    this.L = L
    this
  }

  def setDt(dt: Double): this.type = {
    if(dt <= 0) {
      throw new IllegalArgumentException("one time step must be positive")
    }
    this.dt = dt
    this
  }

  def setRCutoff(rCutoff: Double): this.type = {
    if(rCutoff <= 0) {
      throw new IllegalArgumentException("rCutoff must be positive")
    }
    this.rCutoff = rCutoff * rCutoff
    this
  }

  override private [nbody] def parInfoMap(parInfoI:RDD[Array[Array[Double]]], bcParInfoI:Array[Array[Array[Double]]])
  : RDD[Array[Array[Double]]] = {
    parInfoI.map(p => update(p, bcParInfoI, L)).cache()
  }

  private [nbody] def update(a: Array[Array[Double]], b: Array[Array[Array[Double]]], L:Double) = {
    // Velocity Verlet Integration Algorithm is used to solve second order ordinary differential equations
    newPositionMatrix(
      nBodyInteraction(
        newPositionMatrix(
          nBodyInteraction(a,  b, L)),  b, L), L)
  }

  private def newPositionMatrix(a:Array[Array[Double]], L:Double)={
    for( i <- 0 until a.size ){
      a(i) = newPosition(a(i), L)
    }
    a

  }

  private [nbody] def nBodyInteraction(a:Array[Array[Double]], b:Array[Array[Array[Double]]], L:Double)={
    // a: local particles, b: global particles
    // Compute forces between a and b
    for (i <- 0 until a.size){
      for (j <- 0 until b(0).size * b.size){
        val bi = j / b(0).size
        val bj = j % b(0).size
        if(abs(a(i)(0) - b(bi)(bj)(0)) > 1e-6){
          a(i) = bodyBodyInteraction(a(i), b(bi)(bj), L)
        }
      }
    }
    a
  }

  private [nbody] def bodyBodyInteraction(a:Array[Double], b:Array[Double], L:Double)={
  // Computing weak van der Waals forces, Cutoff = 2.5

    var rx = a(1) - b(1)
    // using periodic boundary conditions
    if(rx > 0.5 * L) {rx = rx - L}
    if(rx < -0.5 * L) {rx = rx + L}

    var ry = a(2) - b(2)
    if(ry > 0.5 * L) {ry = ry - L}
    if(ry < -0.5 * L) {ry = ry + L}

    var rz = a(3) - b(3)
    if(rz > 0.5 * L) {rz = rz - L}
    if(rz < -0.5 * L) {rz = rz + L}

    val r2 = rx*rx + ry*ry + rz*rz

    if( r2 < rCutoff){
      /*
       * Lennard-Jones function
       * f = 24 * ( 2 * math.pow(r2, -7) - math.pow(r2,-4))
       */
      val r2inv = 1/r2
      val r6inv = r2inv * r2inv * r2inv
      val f = 24 * r2inv * r6inv * ( 2 * r6inv - 1 )

      a(4) += rx * f
      a(5) += ry * f
      a(6) += rz * f

      a
    }
    else a

  }

  private [nbody] def newPosition(a:Array[Double], L:Double)={
    // Verlet Integration Algorithm

    for( i <- 1 to 3){
      // update positions
      a(i) += a(i+6) * dt + 0.5 * a(i+3) * dt * dt
      // using periodic boundary conditions
      if(a(i) < 0) {a(i) += L;}
      if(a(i) > L) {a(i) -= L;}
      // update velocities
      a(i+6) += a(i+3) * dt * 0.5
      // set all accelerations to zero
      a(i+3) = 0.0
    }

    a
  }

  override private [nbody] def newPosition(a:Array[Double])={
  // Velocity Verlet Integration Algorithm

    a(7) += a(4) * dt * 0.5
    a(8) += a(5) * dt * 0.5
    a(9) += a(6) * dt * 0.5

    // set all accelerations to zero
    a(4) = 0.0
    a(5) = 0.0
    a(6) = 0.0

    a
  }


}




object DistNBodyDirectMD {

  def run(
           g: RDD[Array[Array[Double]]],
           nParticles: Int,
           slices: Int,
           cycles: Int,
           L: Double,
           checkPointThreshold: Int,
           dt: Double,
           rCutoff: Double) : RDD[Array[Array[Double]]] = {
    new DistNBodyDirectMD().setNParticles(nParticles)
      .setSlices(slices)
      .setCycles(cycles)
      .setL(L)
      .setDt(dt)
      .setRCutoff(rCutoff)
      .setCheckPointThreshold(checkPointThreshold)
      .simulation(g)
  }

  def run(
           g: RDD[Array[Array[Double]]],
           nParticles: Int,
           slices: Int,
           cycles: Int,
           L: Double) : RDD[Array[Array[Double]]] = {
    new DistNBodyDirectMD().setNParticles(nParticles)
      .setSlices(slices)
      .setCycles(cycles)
      .setL(L)
      .simulation(g)


  }

}

