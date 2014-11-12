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

import scala.collection.mutable
import scala.util.Random
import scala.math._

object NbodyBF {

  //####### Data Structure ############
  def generateDatapair = {
    var x: mutable.Set[Array[Array[Double]]] = mutable.Set.empty
    var iii = 0
    val nn = ceil(pow(nparticles, 1.0/3)).toInt
    val L = pow(nparticles/0.8, 1.0/3)  // linear size of cubical volume
    val pa = L / nn     // lattice spacing
    val vscale = 0.1    // maximum initial velocity component


    var lattice = Array.ofDim[Double](nparticles, 3)
    for(k <- 0 until nn){
      for(i <- 0 until nn){
        for(j <- 0 until nn){
          lattice(iii)(0) = (k + 0.5) * pa
          lattice(iii)(1) = (i + 0.5) * pa
          lattice(iii)(2) = (j + 0.5) * pa
          iii += 1
        }
      }
    }
    iii = 0
    while (x.size < slices) {
      val y = Array.ofDim[Double](nparticles/slices, nparameters)
      for (i <- 0 until nparticles/slices){
        y(i)(0)= iii
        y(i)(1)= lattice(iii)(0)
        y(i)(2)= lattice(iii)(1)
        y(i)(3)= lattice(iii)(2)
        y(i)(4)= 0.0
        y(i)(5)= 0.0
        y(i)(6)= 0.0

        y(i)(7)= (rand.nextDouble() * 2 - 1) * vscale
        y(i)(8)= (rand.nextDouble() * 2 - 1) * vscale
        y(i)(9)= (rand.nextDouble() * 2 - 1) * vscale

        iii += 1

      }
      x.+=(y)
    }
    x.toSeq.toArray

  }


  //####### global parameters ############
  val rand = new Random(42)
  var nparameters = 10
  var nparticles = 0
  var slices = 0
  //  val out = new PrintWriter("record.txt")
  val dt = 0.01
  val rCutoff = 2.5 * 2.5
  //####### global parameters #########END


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
    val sc = new SparkContext(conf)
    sc.setCheckpointDir(args(1))

    nparticles = args(2).toInt
    val cycles = args(3).toInt
    slices = if (args(4).toInt > 2) args(4).toInt else 2
    nparticles = nparticles * nparticles * nparticles
    //
    if(nparticles % slices != 0 || nparticles / slices == 0){
      System.err.println("number of particles % number of threads != 0")
      System.exit(1)
    }


    //  var allparticle = generateDatapair.toArray
    var allparticle1 = sc.parallelize(generateDatapair, slices).cache()
    //#### Use broadcast() ##
    var allparticlebroadcast = sc.broadcast(allparticle1.collect())
    val L = sc.broadcast(pow(nparticles/0.8, 1.0/3))  // linear size of cubical volume

    //    CheckandWrite(allparticle1.collect())

    for( k <- 0 until cycles){
      //###### Use broadcast() ##
      //      allparticle1 = allparticle1.map(p => (p._1, NbodyInteraction(p._2, allparticlebroadcast.value))).map(p => (p._1, NewpositionMatrix(p._2))).cache()
      allparticle1 = allparticle1.map(p => Dodo(p, allparticlebroadcast.value, L.value)).cache()



      //      allparticle1 = allparticle1.map(p => (p._1, NbodyInteraction(p._2, allparticlebroadcast.value))).map(p => (p._1, NewpositionMatrix_second(p._2))).cache()

      //#### Cut the lineage ! To prevent from StackOverFlowError ####
      if(k % 150 == 149 || k == cycles - 1) {
        allparticle1.checkpoint()
        allparticle1.count()
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
      allparticlebroadcast = sc.broadcast(allparticle1.collect())


      //#### Pipeline Method #####
      /*
            for( i <- 0 to slices - 1){
              allparticle1 = allparticle1.join(allparticle2).map(p => (p._1, NbodyInteraction(p._2._1 ,p._2._2))) //.map(p => (p._1, p._2._1))
              allparticle2 = allparticle2.map(p => if(p._1 == slices - 1) (0, p._2) else (p._1 + 1, p._2))

            }
            allparticle1 = allparticle1.map(p => (p._1, NewpositionMatrix(p._2)))
      */
      //#### Pipeline Method ##END

    }
    //   out.close()
    println("iteration number: " + cycles)
    println("This N-body simulation is completed!")

  }

  def Dodo(a:Array[Array[Double]], b:Array[Array[Array[Double]]], L:Double) = {
    NewpositionMatrix(NbodyInteraction(NewpositionMatrix_second(NbodyInteraction(a,  b, L)),  b, L), L)
  }

  def CheckandWrite(a:Array[Array[Array[Double]]])={

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


  def BodybodyInteraction(a:Array[Double], b:Array[Double], L:Double)={
    /*
        var r = new Array[Double](3)
        var r2 = 0.0
        for(i <- 0 to 2){
          r(i) = a(i+1) - b(i+1)
          if(r(i) > 0.5*L){r(i) -= L}
          if(r(i) < -0.5*L){r(i) += L}
          r2 += r(i) * r(i)
        }
    */
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
      /*

            a(4) += r(0) * f
            a(5) += r(1) * f
            a(6) += r(2) * f
            */
      a(4) += rx * f
      a(5) += ry * f
      a(6) += rz * f

      a
    }
    else a

  }

  def NbodyInteraction(a:Array[Array[Double]], b:Array[Array[Array[Double]]], L:Double)={
    for (i <- 0 until a.size){
      //      for (j <- 0 to nparticles - 1){
      //        val bi = (j / (nparticles / slices).toInt).toInt
      //        val bj = j % (nparticles / slices).toInt
      for (j <- 0 until b(0).size * b.size){
        val bi = j / b(0).size
        val bj = j % b(0).size
        if(abs(a(i)(0) - b(bi)(bj)(0)) > 1e-6){
          //       if(a(i)(0).toInt != b(bi)(bj)(0).toInt){
          a(i) = BodybodyInteraction(a(i), b(bi)(bj), L)
        }
      }
    }
    a
  }

  //###### Pipeline Method
  /*
    def NbodyInteraction(a:Array[Array[Double]], b:Array[Array[Double]])={
      for (i <- 0 to nparticles/slices - 1){
        for (j <- 0 to nparticles/slices - 1){
          a(i) = BodybodyInteraction(a(i), b(j))
        }
      }
      a
    }
  */

  def NewpositionMatrix(a:Array[Array[Double]], L:Double)={
    for( i <- 0 until a.size ){
      a(i) = Newposition_MD(a(i), L)
    }
    a

  }

  def NewpositionMatrix_second(a:Array[Array[Double]])={
    for( i <- 0 until a.size ){
      a(i) = Newposition_MD_second(a(i))
    }
    a

  }

  def Newposition_MD(a:Array[Double], L:Double)={


    for( i <- 1 to 3){

      a(i) += a(i+6) * dt + 0.5 * a(i+3) * dt * dt
      if(a(i) < 0) {a(i) += L;}
      if(a(i) > L) {a(i) -= L;}
      a(i+6) += a(i+3) * dt * 0.5
      a(i+3) = 0.0
    }

    a
  }

  def Newposition_MD_second(a:Array[Double])={

    a(7) += a(4) * dt * 0.5
    a(8) += a(5) * dt * 0.5
    a(9) += a(6) * dt * 0.5

    a(4) = 0.0
    a(5) = 0.0
    a(6) = 0.0


    a
  }

}


