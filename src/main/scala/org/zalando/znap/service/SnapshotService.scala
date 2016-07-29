package org.zalando.znap.service

import akka.actor.{Actor, Props}


object SnapshotService {
  type Commit = (String) => Unit

  def spec() =
    Props(new SnapshotService())
}


class SnapshotService extends PoolService {
  override def props: Props = Props(new SnapshotRequest())
}


class SnapshotRequest extends Actor {
  def receive = {
    case commit: SnapshotService.Commit =>
      commit("snapshot")
      context.stop(self)
  }
}
