import cats.data.Kleisli
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import com.comcast.ip4s.{host, Port}
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import org.http4s.*
import org.http4s.circe.*
import org.http4s.dsl.io.*
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.{Server, ServerBuilder}
import org.typelevel.ci.*

import java.util.UUID

case class TorrentInfo(
  name: String,
  infoHash: String,
  phase: String,
  downloadPath: Option[String]
)

trait TorrentOps[F[_]]:
  def list: F[List[TorrentInfo]]
  def create(infoHash: String, name: String, downloadPath: Option[String]): F[Unit]
  def delete(infoHash: String): F[Unit]

case class MagnetInfo(infoHash: String, displayName: Option[String])

object TransmissionServer:

  def stream(ops: TorrentOps[IO], port: Int): Stream[IO, Nothing] =
    Stream.resource(server(ops, port)).flatMap(_ => Stream.never[IO])

  def server(ops: TorrentOps[IO], port: Int): Resource[IO, org.http4s.server.Server] =
    val app = routes(ops)
    EmberServerBuilder
      .default[IO]
      .withHost(host"0.0.0.0")
      .withPort(Port.fromInt(port).getOrElse(sys.error("invalid port")))
      .withHttpApp(app)
      .build

  private val SessionHeader = ci"X-Transmission-Session-Id"

  def routes(ops: TorrentOps[IO]): HttpApp[IO] =
    import org.http4s.dsl.io.{*, given}

    Kleisli:
      case req @ POST -> Root / "transmission" / "rpc" =>
        val sessionId = req.headers.get(SessionHeader).map(_.head.value)
        for
          _ <- IO.println(s"[RPC] POST /transmission/rpc session-id=$sessionId")
          resp <- sessionId match
            case None =>
              val sid = UUID.randomUUID().toString
              IO.println(s"[RPC] No session-id, returning 409 with $sid") *>
              Conflict(
                Json.obj("result" -> Json.fromString("needs-session-id")),
                Headers(Header.Raw(SessionHeader, sid))
              )
            case Some(_) =>
              for
                body <- req.as[Json]
                _ <- IO.println(s"[RPC] Body: ${body.noSpaces}")
                result <- handleRpc(ops, body)
                _ <- IO.println(s"[RPC] Result: ${result.noSpaces}")
                response <- Ok(result)
              yield response
        yield resp
      case req @ GET -> Root / "transmission" / "rpc" =>
        val sid = UUID.randomUUID().toString
        IO.println(s"[RPC] GET /transmission/rpc, returning 409 with $sid") *>
        Conflict(
          Json.obj("result" -> Json.fromString("needs-session-id")),
          Headers(Header.Raw(SessionHeader, sid))
        )
      case req @ _ =>
        IO.println(s"[RPC] Unmatched: ${req.method} ${req.uri}") *>
        NotFound()

  def handleRpc(ops: TorrentOps[IO], body: Json): IO[Json] =
    val method = body.hcursor.get[String]("method").getOrElse("")
    val arguments = body.hcursor.get[Json]("arguments").getOrElse(Json.obj())
    method match
      case "session-get"      => IO.pure(sessionGet)
      case "torrent-get"      => torrentGet(ops, arguments)
      case "torrent-add"      => torrentAdd(ops, arguments)
      case "torrent-remove"   => torrentRemove(ops, arguments)
      case "torrent-set"      => IO.pure(success)
      case "queue-move-top"   => IO.pure(success)
      case _                  => IO.pure(Json.obj("result" -> Json.fromString(s"method '$method' not supported")))

  private val success: Json =
    Json.obj("result" -> Json.fromString("success"), "arguments" -> Json.obj())

  private val sessionGet: Json =
    Json.obj(
      "result" -> Json.fromString("success"),
      "arguments" -> Json.obj(
        "rpc-version" -> Json.fromInt(15),
        "rpc-version-minimum" -> Json.fromInt(1),
        "version" -> Json.fromString("4.0.0 (TorrentDam)"),
        "download-dir" -> Json.fromString("downloading"),
        "seedRatioLimit" -> Json.fromDoubleOrNull(2.0),
        "seedRatioLimited" -> Json.fromBoolean(false),
        "idle-seeding-limit" -> Json.fromInt(30),
        "idle-seeding-limit-enabled" -> Json.fromBoolean(false)
      )
    )

  private def torrentGet(ops: TorrentOps[IO], arguments: Json): IO[Json] =
    ops.list.map { torrents =>
      val fields = arguments.hcursor.get[List[String]]("fields").getOrElse(Nil)
      val mapped = torrents.map(t => torrentToJson(t, fields))
      Json.obj(
        "result" -> Json.fromString("success"),
        "arguments" -> Json.obj(
          "torrents" -> Json.fromValues(mapped)
        )
      )
    }

  private def torrentToJson(t: TorrentInfo, fields: List[String]): Json =
    val isFinished = t.phase == "Succeeded"
    val all: Map[String, Json] = Map(
      "id" -> Json.fromInt(t.infoHash.hashCode),
      "hashString" -> Json.fromString(t.infoHash),
      "name" -> Json.fromString(t.name),
      "downloadDir" -> Json.fromString(t.downloadPath.getOrElse("downloading")),
      "totalSize" -> Json.fromLong(1),
      "leftUntilDone" -> Json.fromLong(if isFinished then 0 else 1),
      "isFinished" -> Json.fromBoolean(isFinished),
      "eta" -> Json.fromLong(-1),
      "status" -> Json.fromInt(phaseToStatus(t.phase)),
      "secondsDownloading" -> Json.fromLong(0),
      "secondsSeeding" -> Json.fromLong(0),
      "errorString" -> Json.fromString(""),
      "uploadedEver" -> Json.fromLong(0),
      "downloadedEver" -> Json.fromLong(0),
      "seedRatioLimit" -> Json.fromDoubleOrNull(0.0),
      "seedRatioMode" -> Json.fromInt(0),
      "seedIdleLimit" -> Json.fromLong(0),
      "seedIdleMode" -> Json.fromInt(0),
      "fileCount" -> Json.fromInt(0),
      "file-count" -> Json.fromInt(0),
      "labels" -> Json.fromValues(List(Json.fromString("radarr")))
    )
    if fields.isEmpty then Json.fromFields(all.toSeq) else Json.fromFields(fields.flatMap(f => all.get(f).map(f -> _)))

  private def phaseToStatus(phase: String): Int =
    phase match
      case "Pending"   => 3
      case "Running"   => 4
      case "Succeeded" => 0
      case "Failed"    => 0
      case _           => 0

  private def torrentAdd(ops: TorrentOps[IO], arguments: Json): IO[Json] =
    val filename = arguments.hcursor.get[String]("filename").getOrElse("")
    parseMagnet(filename) match
      case Some(m) =>
        val name = m.displayName.getOrElse(m.infoHash.toLowerCase)
        val downloadPath = s"downloading/$name"
        ops.create(m.infoHash, name, Some(downloadPath)).as(
          Json.obj(
            "result" -> Json.fromString("success"),
            "arguments" -> Json.obj(
              "torrent-added" -> Json.obj(
                "id" -> Json.fromInt(m.infoHash.hashCode),
                "hashString" -> Json.fromString(m.infoHash),
                "name" -> Json.fromString(name)
              )
            )
          )
        )
      case None =>
        IO.pure(Json.obj("result" -> Json.fromString("could not parse magnet URI")))

  private def torrentRemove(ops: TorrentOps[IO], arguments: Json): IO[Json] =
    val ids = arguments.hcursor.get[List[String]]("ids").getOrElse(Nil)
    ids.traverse(ops.delete).as(success)

  private def parseMagnet(magnet: String): Option[MagnetInfo] =
    Uri.fromString(magnet).toOption.flatMap { uri =>
      val infoHash = uri.query.params.get("xt")
        .collect { case s"urn:btih:$hash" if hash.length == 40 => hash.toUpperCase }
      val displayName = uri.query.params.get("dn")
      infoHash.map(hash => MagnetInfo(hash, displayName))
    }