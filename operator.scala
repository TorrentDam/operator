//> using scala 3.7.2
//> using jvm 21

//> using dep dev.hnaderi::scala-k8s-http4s::0.23.0
//> using dep dev.hnaderi::scala-k8s-http4s-ember::0.23.0
//> using dep dev.hnaderi::scala-k8s-circe::0.23.0
//> using dep org.http4s::http4s-circe::0.23.30

import cats.effect.IO
import cats.effect.IOApp
import dev.hnaderi.k8s.*
import dev.hnaderi.k8s.circe.*
import dev.hnaderi.k8s.client.http4s.EmberKubernetesClient
import dev.hnaderi.k8s.client.APIGroupAPI
import dev.hnaderi.k8s.utils.*
import fs2.Stream
import io.circe.Json
import org.http4s.circe.*

object OperatorApp extends IOApp.Simple {
  def run: IO[Unit] =
    val client = EmberKubernetesClient[IO].defaultConfig[Json]
    Stream
      .resource(client)
      .flatMap(TorrentAPI().list.listen)
      .evalMap(event => IO(println(event)))
      .compile
      .drain
}

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
  spec: TorrentSpec
)

object Torrent {
  given Encoder[Torrent] = new Encoder[Torrent] {
    def apply[T: Builder](o: Torrent): T =
      val obj = ObjectWriter[T]()
      obj
        .write("kind", "Torrent")
        .write("apiVersion", "torrent.TorrentDam.github.com/v1")
        .write("spec", o.spec)
        .build
  }

  given Decoder[Torrent] = new Decoder[Torrent] {
    def apply[T: Reader](t: T): Either[String, Torrent] =
      for
        obj <- ObjectReader(t)
        spec <- obj.read[TorrentSpec]("spec")
      yield Torrent(spec)
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
