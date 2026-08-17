//> using scala 3.8.3
//> using jvm 21
//> using javaOpt --add-exports java.base/jdk.internal.vm=ALL-UNNAMED

//> using dep dev.hnaderi::scala-k8s-http4s::0.27.0
//> using dep dev.hnaderi::scala-k8s-http4s-ember::0.27.0
//> using dep dev.hnaderi::scala-k8s-circe::0.27.0
//> using dep org.http4s::http4s-circe::0.23.33
//> using dep org.typelevel::cats-effect::3.7.0
//> using dep org.typelevel::cats-effect-direct::1.0.0

import cats.effect.direct.*
import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.all.given
import cats.Eq
import com.sun.tools.javac.util.Assert.error
import dev.hnaderi.k8s.circe.*
import dev.hnaderi.k8s.client.apis.corev1.PodAPI
import dev.hnaderi.k8s.client.apis.api_extensions.CustomResourceAPI
import dev.hnaderi.k8s.client.http4s.EmberKubernetesClient
import dev.hnaderi.k8s.client.http4s.KClient
import dev.hnaderi.k8s.client.{APIGroupAPI, ErrorResponse, ErrorStatus, HttpClient, WatchEvent, WatchEventType}
import dev.hnaderi.k8s.implicits.convertToOption
import dev.hnaderi.k8s.utils.*
import dev.hnaderi.yaml4s.SnakeYaml
import dev.hnaderi.yaml4s.YAML
import fs2.Stream
import fs2.io.file.{Files, Path}
import io.circe.Json
import io.k8s.api.core.v1.Container
import io.k8s.api.core.v1.EnvVar
import io.k8s.api.core.v1.PersistentVolumeClaimVolumeSource
import io.k8s.api.core.v1.Pod
import io.k8s.api.core.v1.PodSpec
import io.k8s.api.core.v1.ResourceRequirements
import io.k8s.api.core.v1.Volume
import io.k8s.api.core.v1.VolumeMount
import io.k8s.apiextensions_apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition
import io.k8s.apimachinery.pkg.api.resource.Quantity
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import org.http4s.circe.*
import org.http4s.ember.client.EmberClientBuilder

import scala.concurrent.duration.Duration

object OperatorApp extends IOApp.Simple:

  def run: IO[Unit] =
    val emberConfig = EmberClientBuilder.default[IO].withIdleConnectionTime(Duration.Inf)
    val client = EmberKubernetesClient[IO](emberConfig).defaultConfig[Json]
    client.use(operatorLogic)

  def operatorLogic(client: KClient[IO]): IO[Unit] = async[IO]:
    registerCustomResource(client).await
    val operator = Operator(client)
    TorrentClusterAPI
      .list()
      .listen(client)
      .evalTap:
        case WatchEvent(WatchEventType.ADDED | WatchEventType.MODIFIED, torrent) =>
          operator.reconcile(torrent)
        case WatchEvent(WatchEventType.DELETED, torrent) =>
          operator.delete(torrent)
        case _ => IO.unit
      .compile
      .drain
      .await

  def registerCustomResource(client: KClient[IO]): IO[Unit] = async[IO]:
    import dev.hnaderi.k8s.manifest.yamlReader
    try
      CustomResourceAPI.get("torrents.torrentdam.github.com").send(client).await
      IO.println("CRD exists").await
    catch
      case ErrorResponse(error = ErrorStatus.NotFound) =>
        val crdString = Files[IO].readUtf8(Path("crd.yaml")).compile.foldMonoid.await
        val crdYaml = IO.fromEither(SnakeYaml.parse[YAML](crdString)).await
        val crd = IO.fromEither(crdYaml.decodeTo[CustomResourceDefinition].left.map(msg => Throwable(msg))).await
        CustomResourceAPI.create(crd).send(client).await
        IO.println("CRD created").await

end OperatorApp

case class TorrentSpec(
  infoHash: String,
  pvcName: String,
  dhtNode: String,
  downloadPath: String = "/data"
)

object TorrentSpec {
  given Encoder[TorrentSpec] = new Encoder[TorrentSpec] {
    def apply[T: Builder](o: TorrentSpec): T =
      val obj = ObjectWriter[T]()
      obj
        .write("infoHash", o.infoHash)
        .write("pvcName", o.pvcName)
        .write("dhtNode", o.dhtNode)
        .write("downloadPath", o.downloadPath)
        .build
  }

  given Decoder[TorrentSpec] = new Decoder[TorrentSpec] {
    def apply[T: Reader](t: T): Either[String, TorrentSpec] =
      for
        obj <- ObjectReader(t)
        infoHash <- obj.read[String]("infoHash")
        pvcName <- obj.read[String]("pvcName")
        dhtNode <- obj.read[String]("dhtNode")
      yield
        val downloadPath = obj.read[String]("downloadPath").toOption.getOrElse("/data")
        TorrentSpec(infoHash, pvcName, dhtNode, downloadPath)
  }
}

case class Torrent(
  spec: TorrentSpec,
  metadata: ObjectMeta
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

class Operator(client: KClient[IO]):

  def reconcile(resource: Torrent): IO[Unit] = async[IO]:
    val namespace = resource.metadata.namespace.getOrElse("default")
    val podAPI = PodAPI(namespace)
    val desired = getPod(resource)
    val currentPods = podAPI.list().send(client).await
    val currentPod = currentPods.items.find(pod => pod.metadata.exists(_.name == resource.metadata.name))
    currentPod match
      case Some(current) =>
        podAPI.replace(desired.metadata.get.name.get, desired)
        IO.println(s"Replaced").await
      case None =>
        podAPI.create(desired).send(client).await
        IO.println("Created").await

  def delete(resource: Torrent): IO[Unit] = async[IO]:
    val namespace = resource.metadata.namespace.getOrElse("default")
    val podAPI = PodAPI(namespace)
    val pod = getPod(resource)
    for
      metadata <- pod.metadata
      name <- metadata.name
    do
      podAPI.delete(name).send(client).void.await
      IO.println("Deleted").await

  private def getPod(resource: Torrent): Pod =
    Pod(
      metadata = ObjectMeta(
        name = resource.metadata.name,
        namespace = resource.metadata.namespace
      ).some,
      spec = PodSpec(
        containers = Seq(
          Container(
            name = "torrentdam",
            image = "ghcr.io/torrentdam/cmd:latest".some,
            args = Seq(
              "torrent",
              "download",
              "--info-hash",
              resource.spec.infoHash,
              "--dht-node",
              resource.spec.dhtNode
            ).some,
            workingDir = resource.spec.downloadPath.some,
            env = Seq(
              EnvVar(
                name = "INFO_HASH",
                value = resource.spec.infoHash.some
              )
            ).some,
            volumeMounts = Seq(
              VolumeMount(
                name = "data",
                mountPath = resource.spec.downloadPath
              )
            ).some,
            resources = ResourceRequirements(
              requests = Map(
                "cpu" -> Quantity("500m"),
                "memory" -> Quantity("1Gi")
              )
            ).some
          )
        ),
        volumes = Seq(
          Volume(
            name = "data",
            persistentVolumeClaim = PersistentVolumeClaimVolumeSource(
              claimName = resource.spec.pvcName
            ).some
          )
        ).some
      ).some
    )

end Operator
