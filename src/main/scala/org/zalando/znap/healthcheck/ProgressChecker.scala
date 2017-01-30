/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.healthcheck

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.zalando.znap.config._
import org.zalando.znap.metrics.Instrumented
import org.zalando.znap.persistence.dynamo.DynamoDBOffsetReader
import org.zalando.znap.source.nakadi.{NakadiPartitionGetter, NakadiTokens}
import org.zalando.znap.utils.{NoUnexpectedMessages, ThrowableUtils}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class ProgressChecker(tokens: NakadiTokens) extends Actor with NoUnexpectedMessages with ActorLogging with Instrumented {
  import ProgressChecker._

  private val progressHolders = Config.Pipelines.map { pipeline =>
    new ProgressHolder(pipeline, tokens)(context.system)
  }

  override def preStart(): Unit = {
    progressHolders.foreach(_.registerProgress())
    scheduleTick()
  }

  private def scheduleTick(): Unit = {
    val tickInterval = FiniteDuration(1, scala.concurrent.duration.MINUTES)
    implicit val ec = context.dispatcher
    context.system.scheduler.scheduleOnce(tickInterval, self, Tick)
  }

  override def receive: Receive = {
    case Tick =>
      progressHolders.foreach(_.registerProgress())
      scheduleTick()
  }
}

object ProgressChecker {
  val name = "progress-checker"

  private case object Tick

  def props(tokens: NakadiTokens): Props = {
    Props(classOf[ProgressChecker], tokens)
  }


  private class ProgressHolder(pipeline: SnapshotPipeline,
                               tokens: NakadiTokens)
                              (actorSystem: ActorSystem) extends Instrumented {
    private val logger = LoggerFactory.getLogger(classOf[ProgressHolder])

    import scala.concurrent.duration._

    private val awaitDuration = 30.seconds
    private val progressBarSize = 50

    @volatile private var lastValuesPerPartition_OldestAvailableOffset = Map.empty[String, String]
    @volatile private var lastValuesPerPartition_NewestAvailableOffset = Map.empty[String, String]
    @volatile private var lastValuesPerPartition_CurrentOffset = Map.empty[String, String]

    private val getPartitionsFunc = pipeline.source match {
      case nakadiSource: NakadiSource =>
        val partitionGetter = new NakadiPartitionGetter(nakadiSource, tokens)(actorSystem)
        partitionGetter.getPartitions _

      case s =>
        throw new Exception(s"Source type $s is not supported")
    }

    private val getOffsetFunc = pipeline.offsetPersistence match {
      case dynamoDBOffsetPersistence: DynamoDBOffsetPersistence =>
        val dynamoDBClient = new AmazonDynamoDBClient()
        dynamoDBClient.withEndpoint(dynamoDBOffsetPersistence.uri.toString)
        val dynamoDB = new DynamoDB(dynamoDBClient)

        val dispatcher = actorSystem.dispatchers.lookup(Config.Akka.DynamoDBDispatcher)
        val offsetReader = new DynamoDBOffsetReader(dynamoDBOffsetPersistence, dynamoDB)(dispatcher)
        offsetReader.init()

        offsetReader.getLastOffsets _
    }

    def registerProgress(): Unit = {
      try {
        val partitionsF = getPartitionsFunc()
        val offsetsF = getOffsetFunc()
        val partitions = Await.result(partitionsF, awaitDuration)
        val offsets = Await.result(offsetsF, awaitDuration)

        partitions.sortBy(_.partition).foreach { partition =>
          val start = partition.oldestAvailableOffset
          val end = partition.newestAvailableOffset

          lastValuesPerPartition_OldestAvailableOffset += partition.partition -> partition.oldestAvailableOffset
          lastValuesPerPartition_NewestAvailableOffset += partition.partition -> partition.newestAvailableOffset

          offsets.get(partition.partition) match {
            case Some(offset) =>
              lastValuesPerPartition_CurrentOffset += partition.partition -> offset

              val offsetExt = StringUtils.leftPad(offset, 18, "0")
              val startExt = StringUtils.leftPad(start, 18, "0")
              val endExt = StringUtils.leftPad(end, 18, "0")

              if (offsetExt >= startExt && offsetExt <= endExt) {
                val lengthF = getDistanceBetweenOffsets(start, end)
                val progressF = getDistanceBetweenOffsets(start, offset)
                val length = Await.result(lengthF, awaitDuration)
                val progress = Await.result(progressF, awaitDuration)

                val positionRaw = ((progressBarSize * progress.toDouble) / length).toInt
                val position =
                  if (positionRaw < 0) {
                    0
                  } else if (positionRaw > (progressBarSize - 1)) {
                    progressBarSize - 1
                  } else {
                    positionRaw
                  }

                val msg = s"${pipeline.id} ${partition.partition} [" +
                  "." * position +
                  "*" +
                  ("." * (progressBarSize - position - 1)) +
                  s"] $start - $offset - $end"
                logger.info(msg)
              } else {
                logger.error(s"In ${pipeline.id} ${partition.partition}, the current offset is $offset, available offsets are $start - $end.")
              }
            case _ =>
              val msg = s"${pipeline.id} ${partition.partition} [..................................................] $start - __ - $end"
              logger.info(msg)
          }
        }
      } catch {
        case NonFatal(e) =>
          logger.warn(s"Can't print progress for ${pipeline.id}: ${ThrowableUtils.getStackTraceString(e)}")
      }
    }

    Await.result(getPartitionsFunc(), awaitDuration).sortBy(_.partition).foreach { p =>
      metrics.gauge(s"oldestAvailableOffset-${pipeline.id}-${p.partition}") {
        lastValuesPerPartition_OldestAvailableOffset.getOrElse(p.partition, "None")
      }
      metrics.gauge(s"newestAvailableOffset-${pipeline.id}-${p.partition}") {
        lastValuesPerPartition_NewestAvailableOffset.getOrElse(p.partition, "None")
      }
      metrics.gauge(s"currentOffset-${pipeline.id}-${p.partition}") {
        lastValuesPerPartition_CurrentOffset.getOrElse(p.partition, "None")
      }
    }

    // Prepared for future changes in Nakadi, when there will be
    // a special endpoint for measuing the distance between two offsets.
    private def getDistanceBetweenOffsets(offset1: String, offset2: String): Future[Long] = {
      Future.successful(offset2.toLong - offset1.toLong)
    }
  }
}
