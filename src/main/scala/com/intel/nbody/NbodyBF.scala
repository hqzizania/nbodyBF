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
package com.intel.nbody

import org.apache.spark._
import scala.math._

/**
 * Created by qhuang on 11/13/14.
 */


object NbodyBF {

  def main(args: Array[String]) {

    if (args.length < 5) {
      System.err.println("Usage: LocalNbodyBF <numParticle(x direction)> <time_steps> <slices>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("SparkNbodyBF")
      .setMaster(args(0))
      .set("spark.executor.memory", "120g")
      .set("spark.cores.max", "224")
      .set("CheckpointDir", args(1))
    val sc = new SparkContext(conf)

    val nparticles = args(2).toInt * args(2).toInt * args(2).toInt
    val cycles = args(3).toInt
    val slices = if (args(4).toInt > 2) args(4).toInt else 2
    //
    if (nparticles % slices != 0 || nparticles / slices == 0) {
      System.err.println("number of particles % number of threads != 0")
      System.exit(1)
    }

    val g = new GenLatticeExample(sc, nparticles, slices)
    val nbody = new NbodyBF(sc, g, nparticles, slices, cycles)
    sc.stop()
  }
}

class NbodyBF(sc:SparkContext, g:GenLatticeExample, nparticles:Int, slices:Int, cycles:Int){

    val dt = 0.01
    val rCutoff = 2.5 * 2.5

    var allparticle = sc.parallelize(g.generateData, slices).cache()
    var allparticlebroadcast = sc.broadcast(allparticle.collect())
    val L = sc.broadcast(pow(nparticles/0.8, 1.0/3))  // linear size of cubical volume

    //    CheckandWrite(allparticle.collect())

    for( k <- 0 until cycles){

      allparticle = allparticle.map(p => Update(p, allparticlebroadcast.value, L.value)).cache()


      //#### Cut the lineage ! To prevent from StackOverFlowError ####
      if(k % 150 == 149 || k == cycles - 1) {
        allparticle.checkpoint()
        allparticle.count()
        /*
        println("loop " + (k+1) + ":" )
        if(CheckandWrite(allparticle1.collect())){
//          out.close()
          println("iteration number: " + k)
          println("This N-body simulation is interrupted!")
          return
        }
        */
      }

      //###### Use broadcast() ##
      allparticlebroadcast = sc.broadcast(allparticle.collect())

    }
    //   out.close()
    println("iteration number: " + cycles)
    println("This N-body simulation is completed!")



  private def Update(a:Array[Array[Double]], b:Array[Array[Array[Double]]], L:Double) = {
    NewpositionMatrix(NbodyInteraction(NewpositionMatrix_second(NbodyInteraction(a,  b, L)),  b, L), L)
  }

  private def CheckandWrite(a:Array[Array[Array[Double]]])={

    var balance = true
    for(k <- 0 until a.size){
      for(i <- 0 until a(0).size){
        for(j <- 0 until a(0)(0).size){
          print(a(k)(i)(j) + " ")
        }
        println()
      }
    }

    balance = false
    balance
  }


  private def BodybodyInteraction(a:Array[Double], b:Array[Double], L:Double)={

    var rx = a(1) - b(1)
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
      val r2inv = 1/r2
      val r6inv = r2inv * r2inv * r2inv
      val f = 24 * r2inv * r6inv * ( 2 * r6inv - 1 )
      //      val f = 24 * ( 2 * math.pow(r2, -7) - math.pow(r2,-4))
      a(4) += rx * f
      a(5) += ry * f
      a(6) += rz * f

      a
    }
    else a

  }

  private def NbodyInteraction(a:Array[Array[Double]], b:Array[Array[Array[Double]]], L:Double)={
    for (i <- 0 until a.size){

      for (j <- 0 until b(0).size * b.size){
        val bi = j / b(0).size
        val bj = j % b(0).size
        if(abs(a(i)(0) - b(bi)(bj)(0)) > 1e-6){
          a(i) = BodybodyInteraction(a(i), b(bi)(bj), L)
        }
      }
    }
    a
  }


  private def NewpositionMatrix(a:Array[Array[Double]], L:Double)={
    for( i <- 0 until a.size ){
      a(i) = Newposition_MD(a(i), L)
    }
    a

  }

  private def NewpositionMatrix_second(a:Array[Array[Double]])={
    for( i <- 0 until a.size ){
      a(i) = Newposition_MD_second(a(i))
    }
    a

  }

  private def Newposition_MD(a:Array[Double], L:Double)={


    for( i <- 1 to 3){

      a(i) += a(i+6) * dt + 0.5 * a(i+3) * dt * dt
      if(a(i) < 0) {a(i) += L;}
      if(a(i) > L) {a(i) -= L;}
      a(i+6) += a(i+3) * dt * 0.5
      a(i+3) = 0.0
    }

    a
  }

  private def Newposition_MD_second(a:Array[Double])={

    a(7) += a(4) * dt * 0.5
    a(8) += a(5) * dt * 0.5
    a(9) += a(6) * dt * 0.5

    a(4) = 0.0
    a(5) = 0.0
    a(6) = 0.0


    a
  }

}


