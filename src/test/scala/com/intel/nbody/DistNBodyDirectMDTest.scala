package com.intel.nbody

import com.intel.nbody.Direct.Astro.DistNBodyDirectAstro
import com.intel.nbody.Direct.MD.DistNBodyDirectMD
import org.apache.spark._

import scala.math._

/**
 * Created by qhuang on 12/25/14.
 */

object DistNBodyDirectMDTest{

  /* dt: one time step
     Verlet cutoff on the potential and neighbor list approximation
     rCutoff = Cutoff * Cutoff
   */

  def main(args: Array[String]) {
    println("*****************NbodyBF*******************")
    if (args.length < 5) {
      System.err.println("Usage: NbodyBF <master> <path to directory of generated data> <numParticle(x direction)> <time_steps> <slices>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("SparkNbodyBF")
      .setMaster(args(0))
      .set("spark.executor.memory", "120g")
      .set("spark.cores.max", "224")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir(args(1))

    val nParticles = args(2).toInt * args(2).toInt * args(2).toInt
    val cycles = args(3).toInt
    val slices = if (args(4).toInt > 2) args(4).toInt else 2
    //
    if (nParticles % slices != 0 || nParticles / slices == 0) {
      System.err.println("number of particles % number of threads != 0")
      System.exit(1)
    }
    val L = pow(nParticles/0.8, 1.0/3)  // linear size of cubical volume

    val g = new GenLatticeExample(sc, nParticles, slices)

    val gg = sc.parallelize(g.generateData, slices).cache()

    DistNBodyDirectMD.run(gg, nParticles, slices, cycles, L)

  }
}
