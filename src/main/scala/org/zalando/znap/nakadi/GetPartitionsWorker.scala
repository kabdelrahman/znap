/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.nakadi

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.zalando.scarl.Supervisor
import org.zalando.znap.config.{Config, NakadiSource}
import org.zalando.znap.nakadi.objects.NakadiPartition
import org.zalando.znap.objects.Partition
import org.zalando.znap.service.PoolService
import org.zalando.znap.utils.{NoUnexpectedMessages, TimeoutException}

import scala.concurrent.duration.FiniteDuration

/**
  * Actor that gets partitions information from Nakadi.
  */
class NakadiQueueService(source: NakadiSource, config: Config, oauth: OAuth) extends PoolService {

  override def props: Props =
    Props(classOf[GetPartitionsWorker], source, config, oauth)
}

class GetPartitionsWorker(nakadiSource: NakadiSource,
                          config: Config,
                          oauth: OAuth) extends Actor
    with NoUnexpectedMessages with ActorLogging {

  // TODO robust retries

  import GetPartitionsWorker._
  import akka.pattern.pipe
  import context.dispatcher
  import org.zalando.znap.utils.RichStream._

  val oauthScope = "nakadi"
  var timer: Option[Cancellable] = None

  def waitingForCommand: Receive = {
    case GetPartitionsCommand =>
      implicit val materializer = ActorMaterializer(ActorMaterializerSettings(context.system))
      val http = Http(context.system)

      // Request partitions of the topic.
      val scheme = nakadiSource.uri.getScheme
      val hostAndPort = s"${nakadiSource.uri.getHost}:${nakadiSource.uri.getPort}"
      val uri = s"$scheme://$hostAndPort/event-types/${nakadiSource.eventType}/partitions"
      val authorizationHeader = new Authorization(OAuth2BearerToken(oauth.token(oauthScope)))
      val request = HttpRequest(
        uri = uri,
        headers = List(authorizationHeader))

      http.singleRequest(request).flatMap {
        case HttpResponse(StatusCodes.OK, _, entity, _) =>
          entity.dataBytes.collectAsObject[List[NakadiPartition]]().map { nakadiPartitions =>
            val partitions = nakadiPartitions.map { np =>
              Partition(np.partition, np.oldestAvailableOffset, np.newestAvailableOffset)
            }
            Partitions(partitions)
          }

        case unknownResponse: HttpResponse =>
          unknownResponse.entity.dataBytes.collectAsString().map { content =>
            throw new Exception(s"Error response on getting partitions, ${unknownResponse.status} $content")
          }
      } pipeTo self

      timer = Some(context.system.scheduler.scheduleOnce(
        config.Nakadi.PartitionsReadTimeout, self, Timeout(config.Nakadi.PartitionsReadTimeout)))

      context.become(waitingForResponse(sender()))
  }

  def waitingForResponse(requestSource: ActorRef): Receive = {
    case partitions: Partitions =>
      timer.foreach(_.cancel())
      println(partitions)
      requestSource ! partitions
      context.stop(self)

    case akka.actor.Status.Failure(ex) =>
      timer.foreach(_.cancel())
      throw new NakadiException("Error getting partitions from Nakadi", ex)

    case scala.util.Failure(ex) =>
      timer.foreach(_.cancel())
      throw new NakadiException("Error getting partitions from Nakadi", ex)

    case Timeout(t) =>
      throw new TimeoutException(s"Getting partitions from Nakadi didn't finish in $t.")
  }

  override def receive: Receive = waitingForCommand

  override protected def beforeUnhandled(message: Any): Unit = {
    timer.foreach(_.cancel())
  }
}

object GetPartitionsWorker {
  final case class Timeout(timeout: FiniteDuration)

  case object GetPartitionsCommand

  final case class Partitions(partitions: List[Partition])
}
