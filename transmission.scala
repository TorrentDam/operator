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
import org.legogroup.woof.{given, *}
import org.typelevel.ci.*

import java.util.UUID

case class TorrentInfo(
  name: String,
  infoHash: String,
  phase: String,
  downloadPath: Option[String],
  labels: List[String],
  downloadState: Option[DownloadState] = None
)

trait TorrentOps[F[_]]:
  def list: F[List[TorrentInfo]]
  def create(infoHash: String, name: String, downloadPath: Option[String], labels: List[String]): F[Unit]
  def delete(infoHash: String): F[Unit]

case class MagnetInfo(infoHash: String, displayName: Option[String])

object TransmissionServer:

  def stream(ops: TorrentOps[IO], port: Int)(using logger: Logger[IO]): Stream[IO, Nothing] =
    Stream.resource(server(ops, port)).flatMap(_ => Stream.never[IO])

  def server(ops: TorrentOps[IO], port: Int)(using logger: Logger[IO]): Resource[IO, org.http4s.server.Server] =
    val app = routes(ops)
    EmberServerBuilder
      .default[IO]
      .withHost(host"0.0.0.0")
      .withPort(Port.fromInt(port).getOrElse(sys.error("invalid port")))
      .withHttpApp(app)
      .build

  private val SessionHeader = ci"X-Transmission-Session-Id"

  def routes(ops: TorrentOps[IO])(using logger: Logger[IO]): HttpApp[IO] =
    import org.http4s.dsl.io.{*, given}

    Kleisli:
      case req @ POST -> Root / "transmission" / "rpc" =>
        val sessionId = req.headers.get(SessionHeader).map(_.head.value)
        for
          _ <- Logger[IO].debug(s"POST /transmission/rpc session-id=$sessionId")
          resp <- sessionId match
            case None =>
              val sid = UUID.randomUUID().toString
              Logger[IO].debug(s"No session-id, returning 409 with $sid") *>
              Conflict(
                Json.obj("result" -> Json.fromString("needs-session-id")),
                Headers(Header.Raw(SessionHeader, sid))
              )
            case Some(_) =>
              for
                body <- req.as[Json]
                _ <- Logger[IO].debug(s"RPC Body: ${body.noSpaces}")
                result <- handleRpc(ops, body).handleErrorWith { e =>
                  Logger[IO].error(s"RPC handler error: ${e.getMessage}") *>
                    IO.pure(Json.obj("result" -> Json.fromString(s"internal error: ${e.getMessage}")))
                }
                _ <- Logger[IO].debug(s"RPC Result: ${result.noSpaces}")
                response <- Ok(result)
              yield response
        yield resp
      case req @ GET -> Root / "transmission" / "rpc" =>
        val sid = UUID.randomUUID().toString
        Logger[IO].debug(s"GET /transmission/rpc, returning 409 with $sid") *>
        Conflict(
          Json.obj("result" -> Json.fromString("needs-session-id")),
          Headers(Header.Raw(SessionHeader, sid))
        )
      case req @ _ =>
        Logger[IO].warn(s"Unmatched: ${req.method} ${req.uri}") *>
        NotFound()

  def handleRpc(ops: TorrentOps[IO], body: Json)(using logger: Logger[IO]): IO[Json] =
    val method = body.hcursor.get[String]("method").getOrElse("")
    val arguments = body.hcursor.get[Json]("arguments").getOrElse(Json.obj())
    method match
      case "session-get"      => IO.pure(sessionGet)
      case "torrent-get"      => torrentGet(ops, arguments)
      case "torrent-add"      => torrentAdd(ops, arguments)
      case "torrent-remove"   => torrentRemove(ops, arguments)
      case "torrent-set"      => IO.pure(success)
      case "queue-move-top"   => IO.pure(success)
      case _                  =>
        Logger[IO].warn(s"Unsupported RPC method: '$method'").as(
          Json.obj("result" -> Json.fromString(s"method '$method' not supported"))
        )

  private val success: Json =
    Json.obj("result" -> Json.fromString("success"), "arguments" -> Json.obj())

  private val sessionGet: Json =
    Json.obj(
      "result" -> Json.fromString("success"),
      "arguments" -> Json.obj(
        "rpc-version" -> Json.fromInt(15),
        "rpc-version-minimum" -> Json.fromInt(1),
        "version" -> Json.fromString("4.0.0 (TorrentDam)"),
        "download-dir" -> Json.fromString("/"),
        "seedRatioLimit" -> Json.fromDoubleOrNull(0.0),
        "seedRatioLimited" -> Json.fromBoolean(true),
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
    val ds = t.downloadState
    val isFinished = t.phase == "Succeeded" || ds.exists(_.isFinished)
    val path = java.nio.file.Path.of(t.downloadPath.getOrElse("/"))
    val downloadDir = Option(path.getParent).map(_.toString).filter(_.nonEmpty).getOrElse("/")
    val downloadDirAbs = if (downloadDir.startsWith("/")) downloadDir else "/" + downloadDir
    val name = ds.flatMap(_.name).filter(_.nonEmpty)
      .orElse(Option(path.getFileName).map(_.toString).filter(_.nonEmpty))
      .getOrElse(t.name)
    val totalSize = ds.map(_.totalSize).getOrElse(1L)
    val leftUntilDone = ds.map(_.leftUntilDone).getOrElse(if isFinished then 0L else 1L)
    val downloadedEver = ds.map(_.downloadedBytes).getOrElse(0L)
    val fileCount = ds.map(_ => 1).getOrElse(0)
    val all: Map[String, Json] = Map(
      "id" -> Json.fromInt(t.infoHash.hashCode),
      "hashString" -> Json.fromString(t.infoHash),
      "name" -> Json.fromString(name),
      "downloadDir" -> Json.fromString(downloadDirAbs),
      "totalSize" -> Json.fromLong(totalSize),
      "leftUntilDone" -> Json.fromLong(leftUntilDone),
      "isFinished" -> Json.fromBoolean(isFinished),
      "eta" -> Json.fromLong(-1),
      "status" -> Json.fromInt(phaseToStatus(t.phase)),
      "secondsDownloading" -> Json.fromLong(0),
      "secondsSeeding" -> Json.fromLong(0),
      "errorString" -> Json.fromString(""),
      "uploadedEver" -> Json.fromLong(0),
      "downloadedEver" -> Json.fromLong(downloadedEver),
      "seedRatioLimit" -> Json.fromDoubleOrNull(0.0),
      "seedRatioMode" -> Json.fromInt(0),
      "seedIdleLimit" -> Json.fromLong(0),
      "seedIdleMode" -> Json.fromInt(0),
      "fileCount" -> Json.fromInt(fileCount),
      "file-count" -> Json.fromInt(fileCount),
      "labels" -> Json.fromValues(t.labels.map(Json.fromString))
    )
    if fields.isEmpty then Json.fromFields(all.toSeq) else Json.fromFields(fields.flatMap(f => all.get(f).map(f -> _)))

  private def phaseToStatus(phase: String): Int =
    phase match
      case "Pending"   => 3
      case "Running"   => 4
      case "Succeeded" => 0
      case "Failed"    => 0
      case _           => 0

  private def torrentAdd(ops: TorrentOps[IO], arguments: Json)(using logger: Logger[IO]): IO[Json] =
    val filename = arguments.hcursor.get[String]("filename").getOrElse("")
    parseMagnet(filename) match
      case Some(m) =>
        val name = m.displayName.getOrElse(m.infoHash.toLowerCase)
        val segment = Option(java.nio.file.Path.of(name).getFileName)
          .map(_.toString.trim)
          .filter(_.nonEmpty)
          .getOrElse(m.infoHash.toLowerCase)
        val clientDir = arguments.hcursor.get[String]("download-dir").toOption
          .filter(_.nonEmpty)
        val labels = arguments.hcursor.get[List[String]]("labels").getOrElse(Nil)
        val downloadPath = clientDir.map(dir => s"$dir/$segment").getOrElse(s"/$segment")
        ops.create(m.infoHash, segment, Some(downloadPath), labels).as(
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
        Logger[IO].warn(s"Could not parse magnet URI: $filename").as(
          Json.obj("result" -> Json.fromString("could not parse magnet URI"))
        )

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