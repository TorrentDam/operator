//> using scala 3.7.2
//> using jvm 24

//> using dep dev.hnaderi::scala-k8s-http4s-ember::0.23.0

import dev.hnaderi.k8s.client.APIGroupAPI
import dev.hnaderi.k8s.utils.*

@main def main(): Unit = {}

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
  given Encoder[TorrentList] = new Encoder[TorrentList] {
    def apply[T: Builder](o: TorrentList): T =
      val obj = ObjectWriter[T]()
      obj
        .write("kind", "TorrentList")
        .write("apiVersion", "torrent.TorrentDam.github.com/v1")
        .write("items", o.items)
        .build
  }

  given Decoder[TorrentList] = new Decoder[TorrentList] {
    def apply[T: Reader](t: T): Either[String, TorrentList] =
      for
        obj <- ObjectReader(t)
        items <- obj.read[Seq[Torrent]]("items")
      yield TorrentList(items)
  }
}

object TorrentAPIGroup extends APIGroupAPI("/apis/torrent.TorrentDam.github.com/v1")

object TorrentResourceAPIs
    extends TorrentAPIGroup.ClusterResourceAPI[
      Torrent,
      TorrentList
    ]("torrents")
