//> using scala 3.7.2
//> using jvm 21

//> using dep dev.hnaderi::scala-k8s-http4s::0.23.0
//> using dep dev.hnaderi::scala-k8s-http4s-ember::0.23.0
//> using dep dev.hnaderi::scala-k8s-circe::0.23.0
//> using dep org.http4s::http4s-circe::0.23.30

import cats.Eq
import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.all.given
import dev.hnaderi.k8s.*
import dev.hnaderi.k8s.circe.*
import dev.hnaderi.k8s.client.http4s.{EmberKubernetesClient, KClient}
import dev.hnaderi.k8s.client.{APIGroupAPI, HttpClient, WatchEvent, WatchEventType}
import dev.hnaderi.k8s.client.apis.corev1.PodAPI
import dev.hnaderi.k8s.utils.*
import fs2.Stream
import io.circe.Json
import io.k8s.api.core.v1.{Pod, PodSpec}
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import org.http4s.circe.*
import org.http4s.ember.client.EmberClientBuilder

import scala.concurrent.duration.Duration

object OperatorApp extends IOApp.Simple:

  def run: IO[Unit] =
    val emberConfig = EmberClientBuilder.default[IO].withIdleConnectionTime(Duration.Inf)
    val client = EmberKubernetesClient[IO](emberConfig).defaultConfig[Json]
    client.use(operatorLogic)

  def operatorLogic(client: KClient[IO]): IO[Unit] =
    val podAPI = PodAPI("default")
    TorrentClusterAPI.list()
      .listen(client)
      .collect:
        case WatchEvent(
          WatchEventType.ADDED | WatchEventType.MODIFIED,
          torrent
        ) =>
          val pod = transform(torrent)
          PodAPI(torrent.metadata.namespace.getOrElse("default"))
            .create(pod)
            .send(client)
            .void
        case WatchEvent(
          WatchEventType.DELETED,
          torrent
        ) =>
          PodAPI(torrent.metadata.namespace.getOrElse("default"))
            .delete(torrent.metadata.name.get)
            .send(client)
            .void
      .compile
      .drain

end OperatorApp

case class TorrentSpec(
  infoHash: String
)

object TorrentSpec {
  given Encoder[TorrentSpec] = new Encoder[TorrentSpec] {
    def apply[T: Builder](o: TorrentSpec): T =
      val obj = ObjectWriter[T]()
      obj
        .write("infoHash", o.infoHash)
        .build
  }

  given Decoder[TorrentSpec] = new Decoder[TorrentSpec] {
    def apply[T: Reader](t: T): Either[String, TorrentSpec] =
      for
        obj <- ObjectReader(t)
        infoHash <- obj.read[String]("infoHash")
      yield TorrentSpec(infoHash)
  }
}

case class Torrent(
  spec: TorrentSpec,
  metadata : ObjectMeta
)

object Torrent {
  given Eq[Torrent] = Eq.fromUniversalEquals

  given Encoder[Torrent] = new Encoder[Torrent] {
    def apply[T: Builder](o: Torrent): T =
      val obj = ObjectWriter[T]()
      obj
        .write("kind", "Torrent")
        .write("apiVersion", "torrent.TorrentDam.github.com/v1")
        .write("spec", o.spec)
        .write("metadata", o.metadata)
        .build
  }

  given Decoder[Torrent] = new Decoder[Torrent] {
    def apply[T: Reader](t: T): Either[String, Torrent] =
      for
        obj <- ObjectReader(t)
        spec <- obj.read[TorrentSpec]("spec")
        metadata <- obj.read[ObjectMeta]("metadata")
      yield Torrent(spec, metadata)
  }
}

case class TorrentList(
  items: Seq[Torrent]
)

object TorrentList {
  given Decoder[TorrentList] = new Decoder[TorrentList] {
    def apply[T: Reader](t: T): Either[String, TorrentList] =
      for
        obj <- ObjectReader(t)
        items <- obj.read[Seq[Torrent]]("items")
      yield TorrentList(items)
  }
}

object TorrentAPIGroup extends APIGroupAPI("/apis/torrentdam.github.com/v1")

object TorrentAPI
    extends TorrentAPIGroup.NamespacedResourceAPI[
      Torrent,
      TorrentList
    ]("torrents")

class TorrentAPI(val namespace: String = "default") extends TorrentAPI.NamespacedAPIBuilders
object TorrentClusterAPI extends TorrentAPI.ClusterwideAPIBuilders


def transform(resource: Torrent): Pod = {
  Pod(
    metadata = ObjectMeta(
      name = resource.metadata.name,
      namespace = resource.metadata.namespace,
    ).some,
    spec = PodSpec(
      containers = Seq(
        io.k8s.api.core.v1.Container(
          name = "torrentdam",
          image = "nginx:latest".some
        )
      ),
    ).some
  )
}

def reconcile(pod: Pod): IO[Unit] = IO.println(pod)
